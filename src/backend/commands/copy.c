/*-------------------------------------------------------------------------
 *
 * copy.c
 *		Implements the COPY utility command
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/copy.c,v 1.273 2006/10/06 17:13:58 petere Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/file.h>

#include "access/fileam.h"
#include "access/filesplit.h"
#include "access/heapam.h"
#include "access/aosegfiles.h"
#include "access/appendonlywriter.h"
#include "access/xact.h"
#include "catalog/gp_policy.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/catalog.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbparquetam.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbsharedstorageop.h"
#include "commands/copy.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "parser/parse_relation.h"
#include "postmaster/identity.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/builtins.h"

#include "cdb/cdbvars.h"
#include "cdb/cdblink.h"
#include "cdb/cdbcopy.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbrelsize.h"
#include "cdb/cdbvarblock.h"
#include "cdb/cdbbufferedappend.h"
#include "commands/vacuum.h"
#include "utils/lsyscache.h"
#include "nodes/makefuncs.h"
#include "postmaster/autovacuum.h"
#include "cdb/dispatcher.h"

/* DestReceiver for COPY (SELECT) TO */
typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	CopyState	cstate;			/* CopyStateData for the command */
} DR_copy;

/* non-export function prototypes */
static void DoCopyTo(CopyState cstate);
extern void CopyToDispatch(CopyState cstate);
static void CopyTo(CopyState cstate);
extern void CopyFromDispatch(CopyState cstate, List *err_segnos);
static void CopyFrom(CopyState cstate);
static char *CopyReadOidAttr(CopyState cstate, bool *isnull);
static void CopyAttributeOutText(CopyState cstate, char *string);
static void CopyAttributeOutCSV(CopyState cstate, char *string,
								bool use_quote, bool single_attr);
static bool DetectLineEnd(CopyState cstate, size_t bytesread  __attribute__((unused)));
static void CopyReadAttributesTextNoDelim(CopyState cstate, bool *nulls,
										  int num_phys_attrs, int attnum);

/* Low-level communications functions */
static void SendCopyBegin(CopyState cstate);
static void ReceiveCopyBegin(CopyState cstate);
static void SendCopyEnd(CopyState cstate);
static void CopySendData(CopyState cstate, const void *databuf, int datasize);
static void CopySendString(CopyState cstate, const char *str);
static void CopySendChar(CopyState cstate, char c);
static int	CopyGetData(CopyState cstate, void *databuf, int datasize);

/* byte scaning utils */
static char *scanTextLine(CopyState cstate, const char *s, char c, size_t len);
static char *scanCSVLine(CopyState cstate, const char *s, char c1, char c2, char c3, size_t len);

static void CopyExtractRowMetaData(CopyState cstate);
static void preProcessDataLine(CopyState cstate);
static void concatenateEol(CopyState cstate);
static char *escape_quotes(const char *src);
static void attr_get_key(CopyState cstate, CdbCopy *cdbCopy, int original_lineno_for_qe,
						 unsigned int target_seg, AttrNumber p_nattrs, AttrNumber *attrs,
						 Form_pg_attribute *attr_descs, int *attr_offsets, bool *attr_nulls,
						 FmgrInfo *in_functions, Oid *typioparams, Datum *values);
static void copy_in_error_callback(void *arg);
static void CopyInitPartitioningState(EState *estate);
static void CopyInitDataParser(CopyState cstate);
static bool CopyCheckIsLastLine(CopyState cstate);

/* ==========================================================================
 * The follwing macros aid in major refactoring of data processing code (in
 * CopyFrom(+Dispatch)). We use macros because in some cases the code must be in
 * line in order to work (for example elog_dismiss() in PG_CATCH) while in
 * other cases we'd like to inline the code for performance reasons.
 *
 * NOTE that an almost identical set of macros exists in fileam.c. If you make
 * changes here you may want to consider taking a look there as well.
 * ==========================================================================
 */

#define RESET_LINEBUF \
cstate->line_buf.len = 0; \
cstate->line_buf.data[0] = '\0'; \
cstate->line_buf.cursor = 0;

#define RESET_ATTRBUF \
cstate->attribute_buf.len = 0; \
cstate->attribute_buf.data[0] = '\0'; \
cstate->attribute_buf.cursor = 0;

#define RESET_LINEBUF_WITH_LINENO \
line_buf_with_lineno.len = 0; \
line_buf_with_lineno.data[0] = '\0'; \
line_buf_with_lineno.cursor = 0;

/*
 * A data error happened. This code block will always be inside a PG_CATCH()
 * block right when a higher stack level produced an error. We handle the error
 * by checking which error mode is set (SREH or all-or-nothing) and do the right
 * thing accordingly. Note that we MUST have this code in a macro (as opposed
 * to a function) as elog_dismiss() has to be inlined with PG_CATCH in order to
 * access local error state variables.
 *
 * changing me? take a look at FILEAM_HANDLE_ERROR in fileam.c as well.
 */
#define COPY_HANDLE_ERROR \
if (cstate->errMode == ALL_OR_NOTHING) \
{ \
	/* re-throw error and abort */ \
	if (Gp_role == GP_ROLE_DISPATCH) \
		cdbCopyEnd(cdbCopy); \
	PG_RE_THROW(); \
} \
else \
{ \
	/* SREH - release error state and handle error */ \
\
	ErrorData	*edata; \
	MemoryContext oldcontext;\
	bool	rawdata_is_a_copy = false; \
	cur_row_rejected = true; \
\
	/* SREH must only handle data errors. all other errors must not be caught */\
	if (ERRCODE_TO_CATEGORY(elog_geterrcode()) != ERRCODE_DATA_EXCEPTION)\
	{\
		/* re-throw error and abort */ \
		if (Gp_role == GP_ROLE_DISPATCH) \
			cdbCopyEnd(cdbCopy); \
		PG_RE_THROW(); \
	}\
\
	/* save a copy of the error info */ \
	oldcontext = MemoryContextSwitchTo(cstate->cdbsreh->badrowcontext);\
	edata = CopyErrorData();\
	MemoryContextSwitchTo(oldcontext);\
\
	if (!elog_dismiss(DEBUG5)) \
		PG_RE_THROW(); /* <-- hope to never get here! */ \
\
	if (Gp_role == GP_ROLE_DISPATCH)\
	{\
		Insist(cstate->err_loc_type == ROWNUM_ORIGINAL);\
		cstate->cdbsreh->rawdata = (char *) palloc(strlen(cstate->line_buf.data) * \
												   sizeof(char) + 1 + 24); \
\
		rawdata_is_a_copy = true; \
		sprintf(cstate->cdbsreh->rawdata, "%d%c%d%c%s", \
			    original_lineno_for_qe, \
				COPY_METADATA_DELIM, \
				cstate->line_buf_converted, \
				COPY_METADATA_DELIM, \
				cstate->line_buf.data);	\
	}\
	else\
	{\
		/* truncate trailing eol chars if we need to store this row in errtbl */ \
		if (cstate->cdbsreh->errtbl) \
			truncateEol(&cstate->line_buf, cstate->eol_type); \
\
		if (Gp_role == GP_ROLE_EXECUTE)\
		{\
			/* if line has embedded rownum, update the cursor to the pos right after */ \
			Insist(cstate->err_loc_type == ROWNUM_EMBEDDED);\
			cstate->line_buf.cursor = 0;\
			if(!cstate->md_error) \
				CopyExtractRowMetaData(cstate); \
		}\
\
		cstate->cdbsreh->rawdata = cstate->line_buf.data + cstate->line_buf.cursor; \
	}\
\
	cstate->cdbsreh->is_server_enc = cstate->line_buf_converted; \
	cstate->cdbsreh->linenumber = cstate->cur_lineno; \
	cstate->cdbsreh->processed = ++cstate->processed; \
	cstate->cdbsreh->consec_csv_err = cstate->num_consec_csv_err; \
\
	/* set the error message. Use original msg and add column name if available */ \
	if (cstate->cur_attname)\
	{\
		cstate->cdbsreh->errmsg = (char *) palloc((strlen(edata->message) + \
												  strlen(cstate->cur_attname) + \
												  10 + 1) * sizeof(char)); \
		sprintf(cstate->cdbsreh->errmsg, "%s, column %s", \
				edata->message, \
				cstate->cur_attname); \
	}\
	else\
	{\
		cstate->cdbsreh->errmsg = pstrdup(edata->message); \
	}\
\
	/* after all the prep work let cdbsreh do the real work */ \
	HandleSingleRowError(cstate->cdbsreh); \
\
	/* cleanup any extra memory copies we made */\
	if (rawdata_is_a_copy) \
		pfree(cstate->cdbsreh->rawdata); \
	if (!IsRejectLimitReached(cstate->cdbsreh)) \
		pfree(cstate->cdbsreh->errmsg); \
\
	MemoryContextReset(cstate->cdbsreh->badrowcontext);\
\
}

/*
 * if in SREH mode and data error occured it was already handled in
 * COPY_HANDLE_ERROR. Therefore, skip to the next row before attempting
 * to do any further processing on this one. There's a QE and QD versions
 * since the QE doesn't have a linebuf_with_lineno stringInfo.
 */
#define QD_GOTO_NEXT_ROW \
RESET_LINEBUF_WITH_LINENO; \
RESET_LINEBUF; \
cur_row_rejected = false; /* reset for next run */ \
continue; /* move on to the next data line */

#define QE_GOTO_NEXT_ROW \
RESET_LINEBUF; \
cur_row_rejected = false; /* reset for next run */ \
cstate->cur_attname = NULL;\
continue; /* move on to the next data line */

#define IF_REJECT_LIMIT_REACHED_ABORT \
if (IsRejectLimitReached(cstate->cdbsreh)) \
{\
	char *rejectmsg_normal = "Segment reject limit reached. Aborting operation. Last error was:";\
	char *rejectmsg_csv_unparsable = "Input includes invalid CSV data that corrupts the ability to parse data rows. This usually means several unescaped embedded QUOTE characters. Data is not parsable.Last error was:";\
	char *finalmsg;\
\
	if (CSV_IS_UNPARSABLE(cstate->cdbsreh))\
	{\
		/* the special "csv un-parsable" case */\
		finalmsg = (char *) palloc((strlen(cstate->cdbsreh->errmsg) + \
									strlen(rejectmsg_csv_unparsable) + 12 + 1)\
									* sizeof(char)); \
		sprintf(finalmsg, "%s %s", rejectmsg_csv_unparsable, cstate->cdbsreh->errmsg);\
	}\
	else\
	{\
		/* the normal case */\
		finalmsg = (char *) palloc((strlen(cstate->cdbsreh->errmsg) + \
									strlen(rejectmsg_normal) + 12 + 1)\
									* sizeof(char)); \
		sprintf(finalmsg, "%s %s", rejectmsg_normal, cstate->cdbsreh->errmsg);\
	}\
\
	if (Gp_role == GP_ROLE_DISPATCH) \
		cdbCopyEnd(cdbCopy); \
\
	ereport(ERROR, \
			(errcode(ERRCODE_T_R_GP_REJECT_LIMIT_REACHED), \
			(errmsg("%s", finalmsg) \
			),errOmitLocation(true))); \
}

/*
 * Send copy start/stop messages for frontend copies.  These have changed
 * in past protocol redesigns.
 */
static void
SendCopyBegin(CopyState cstate)
{
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData buf;
		int			natts = list_length(cstate->attnumlist);
		int16		format = 0;
		int			i;

		pq_beginmessage(&buf, 'H');
		pq_sendbyte(&buf, format);		/* overall format */
		pq_sendint(&buf, natts, 2);
		for (i = 0; i < natts; i++)
			pq_sendint(&buf, format, 2);		/* per-column formats */
		pq_endmessage(&buf);
		cstate->copy_dest = COPY_NEW_FE;
	}
	else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
	{
		/* old way */
		pq_putemptymessage('H');
		/* grottiness needed for old COPY OUT protocol */
		pq_startcopyout();
		cstate->copy_dest = COPY_OLD_FE;
	}
	else
	{
		/* very old way */
		pq_putemptymessage('B');
		/* grottiness needed for old COPY OUT protocol */
		pq_startcopyout();
		cstate->copy_dest = COPY_OLD_FE;
	}
}

static void
ReceiveCopyBegin(CopyState cstate)
{
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData buf;
		int			natts = list_length(cstate->attnumlist);
		int16		format = 0;
		int			i;

		pq_beginmessage(&buf, 'G');
		pq_sendbyte(&buf, format);		/* overall format */
		pq_sendint(&buf, natts, 2);
		for (i = 0; i < natts; i++)
			pq_sendint(&buf, format, 2);		/* per-column formats */
		pq_endmessage(&buf);
		cstate->copy_dest = COPY_NEW_FE;
		cstate->fe_msgbuf = makeStringInfo();
	}
	else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
	{
		/* old way */
		pq_putemptymessage('G');
		cstate->copy_dest = COPY_OLD_FE;
	}
	else
	{
		/* very old way */
		pq_putemptymessage('D');
		cstate->copy_dest = COPY_OLD_FE;
	}
	/* We *must* flush here to ensure FE knows it can send. */
	pq_flush();
}

static void
SendCopyEnd(CopyState cstate)
{
	if (cstate->copy_dest == COPY_NEW_FE)
	{
		/* Shouldn't have any unsent data */
		Assert(cstate->fe_msgbuf->len == 0);

		/* Send Copy Done message */
		pq_putemptymessage('c');
	}
	else
	{
		CopySendData(cstate, "\\.", 2);
		/* Need to flush out the trailer (this also appends a newline) */
 	 	CopySendEndOfRow(cstate);
		pq_endcopyout(false);
	}
}


/*----------
 * CopySendData sends output data to the destination (file or frontend)
 * CopySendString does the same for null-terminated strings
 * CopySendChar does the same for single characters
 * CopySendEndOfRow does the appropriate thing at end of each data row
 *  (data is not actually flushed except by CopySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 *----------
 */
static void
CopySendData(CopyState cstate, const void *databuf, int datasize)
{
	if (!cstate->is_copy_in) /* copy out */
	{
		appendBinaryStringInfo(cstate->fe_msgbuf, (char *) databuf, datasize);
	}
	else /* hack for: copy in */
	{
		/* we call copySendData in copy-in to handle results
		 * of default functions that we wish to send from the
		 * dispatcher to the executor primary and mirror segments.
		 * we do so by concatenating the results to line buffer.
		 */
		appendBinaryStringInfo(&cstate->line_buf, (char *) databuf, datasize);
	}
}

static void
CopySendString(CopyState cstate, const char *str)
{
	CopySendData(cstate, (void *) str, strlen(str));
}

static void
CopySendChar(CopyState cstate, char c)
{
	CopySendData(cstate, &c, 1);
}

/* AXG: Note that this will both add a newline AND flush the data.
 * For the dispatcher COPY TO we don't want to use this method since
 * our newlines already exist. We use another new method similar to
 * this one to flush the data
 */
void
CopySendEndOfRow(CopyState cstate)
{
	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:
			/* Default line termination depends on platform */
#ifndef WIN32
			CopySendChar(cstate, '\n');
#else
			CopySendString(cstate, "\r\n");
#endif

			(void) fwrite(fe_msgbuf->data, fe_msgbuf->len,
						  1, cstate->copy_file);
			if (ferror(cstate->copy_file))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to COPY file: %m")));
				break;
		case COPY_OLD_FE:
			/* The FE/BE protocol uses \n as newline for all platforms */
			CopySendChar(cstate, '\n');

			if (pq_putbytes(fe_msgbuf->data, fe_msgbuf->len))
			{
				/* no hope of recovering connection sync, so FATAL */
				ereport(FATAL,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("connection lost during COPY to stdout")));
			}
				break;
		case COPY_NEW_FE:
			/* The FE/BE protocol uses \n as newline for all platforms */
			CopySendChar(cstate, '\n');

			/* Dump the accumulated row as one CopyData message */
			(void) pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
			break;
		case COPY_EXTERNAL_SOURCE:
			/* we don't actually do the write here, we let the caller do it */
#ifndef WIN32
			CopySendChar(cstate, '\n');
#else
			CopySendString(cstate, "\r\n");
#endif
			return; /* don't want to reset msgbug quite yet */
	}

	/* Reset fe_msgbuf to empty */
	fe_msgbuf->len = 0;
	fe_msgbuf->data[0] = '\0';
}

/*
 * AXG: This one is equivalent to CopySendEndOfRow() besides that
 * it doesn't send end of row - it just flushed the data. We need
 * this method for the dispatcher COPY TO since it already has data
 * with newlines (from the executors).
 */
static void
CopyToDispatchFlush(CopyState cstate)
{
	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:

			(void) fwrite(fe_msgbuf->data, fe_msgbuf->len,
						  1, cstate->copy_file);
			if (ferror(cstate->copy_file))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to COPY file: %m")));
			break;
		case COPY_OLD_FE:

			if (pq_putbytes(fe_msgbuf->data, fe_msgbuf->len))
			{
				/* no hope of recovering connection sync, so FATAL */
				ereport(FATAL,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("connection lost during COPY to stdout")));
			}
			break;
		case COPY_NEW_FE:

			/* Dump the accumulated row as one CopyData message */
			(void) pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
			break;
		case COPY_EXTERNAL_SOURCE:
			Insist(false); /* internal error */
			break;

	}

	/* Reset fe_msgbuf to empty */
	fe_msgbuf->len = 0;
	fe_msgbuf->data[0] = '\0';
}


/*
 * CopyGetData reads data from the source (file or frontend)
 * CopyGetChar does the same for single characters
 *
 * CopyGetEof checks if EOF was detected by previous Get operation.
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied by these functions
 *
 * Returns: the number of bytes that were successfully read
 * into the data buffer.
 */
static int
CopyGetData(CopyState cstate, void *databuf, int datasize)
{
	size_t		bytesread = 0;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:
			bytesread = fread(databuf, 1, datasize, cstate->copy_file);
			if (feof(cstate->copy_file))
				cstate->fe_eof = true;
			break;
		case COPY_OLD_FE:
			if (pq_getbytes((char *) databuf, datasize))
			{
				/* Only a \. terminator is legal EOF in old protocol */
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("unexpected EOF on client connection")));
			}
			bytesread += datasize;		/* update the count of bytes that were
										 * read so far */
			break;
		case COPY_NEW_FE:
			while (datasize > 0 && !cstate->fe_eof)
			{
				int			avail;

				while (cstate->fe_msgbuf->cursor >= cstate->fe_msgbuf->len)
				{
					/* Try to receive another message */
					int			mtype;

			readmessage:
					mtype = pq_getbyte();
					if (mtype == EOF)
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("unexpected EOF on client connection")));
					if (pq_getmessage(cstate->fe_msgbuf, 0))
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("unexpected EOF on client connection")));
					switch (mtype)
					{
						case 'd':		/* CopyData */
							break;
						case 'c':		/* CopyDone */
							/* COPY IN correctly terminated by frontend */
							cstate->fe_eof = true;
							return bytesread;
						case 'f':		/* CopyFail */
							ereport(ERROR,
									(errcode(ERRCODE_QUERY_CANCELED),
									 errmsg("COPY from stdin failed: %s",
											pq_getmsgstring(cstate->fe_msgbuf))));
							break;
						case 'H':		/* Flush */
						case 'S':		/* Sync */

							/*
							 * Ignore Flush/Sync for the convenience of
							 * client libraries (such as libpq) that may
							 * send those without noticing that the
							 * command they just sent was COPY.
							 */
							goto readmessage;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("unexpected message type 0x%02X during COPY from stdin",
											mtype)));
							break;
					}
				}
				avail = cstate->fe_msgbuf->len - cstate->fe_msgbuf->cursor;
				if (avail > datasize)
					avail = datasize;
				pq_copymsgbytes(cstate->fe_msgbuf, databuf, avail);
				databuf = (void *) ((char *) databuf + avail);
				bytesread += avail;		/* update the count of bytes that were
										 * read so far */
				datasize -= avail;
			}
			break;
		case COPY_EXTERNAL_SOURCE:
			Insist(false); /* RET read their own data with external_senddata() */
			break;

	}

	
	return bytesread;
}

/*
 * ValidateControlChars
 *
 * These routine is common for COPY and external tables. It validates the
 * control characters (delimiter, quote, etc..) and enforces the given rules.
 *
 * bool copy
 *  - pass true if you're COPY
 *  - pass false if you're an exttab
 *
 * bool load
 *  - pass true for inbound data (COPY FROM, SELECT FROM exttab)
 *  - pass false for outbound data (COPY TO, INSERT INTO exttab)
 */
void ValidateControlChars(bool copy, bool load, bool csv_mode, char *delim,
						char *null_print, char *quote, char *escape,
						List *force_quote, List *force_notnull,
						bool header_line, bool fill_missing, char *newline,
						int num_columns)
{
	bool	delim_off = (pg_strcasecmp(delim, "off") == 0);

	/*
	 * DELIMITER
	 *
	 * Only single-byte delimiter strings are supported. In addition, if the
	 * server encoding is a multibyte character encoding we only allow the
	 * delimiter to be an ASCII character (like postgresql. For more info
	 * on this see discussion and comments in MPP-3756).
	 */
	if (pg_database_encoding_max_length() == 1)
	{
		/* single byte encoding such as ascii, latinx and other */
		if (strlen(delim) != 1 && !delim_off)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("delimiter must be a single byte character, or \'off\'")));
	}
	else
	{
		/* multi byte encoding such as utf8 */
		if ((strlen(delim) != 1 || IS_HIGHBIT_SET(delim[0])) && !delim_off )
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("delimiter must be a single ASCII character, or \'off\'")));
	}

	if (strchr(delim, '\r') != NULL ||
		strchr(delim, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("delimiter cannot be newline or carriage return")));

	if (strchr(null_print, '\r') != NULL ||
		strchr(null_print, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("null representation cannot use newline or carriage return")));

	if (!csv_mode && strchr(delim, '\\') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("delimiter cannot be backslash")));

	if (strchr(null_print, delim[0]) != NULL && !delim_off)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		errmsg("delimiter must not appear in the NULL specification")));

	/*
	 * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
	 * backslash because it would be ambiguous.  We can't allow the other
	 * cases because data characters matching the delimiter must be
	 * backslashed, and certain backslash combinations are interpreted
	 * non-literally by COPY IN.  Disallowing all lower case ASCII letters
	 * is more than strictly necessary, but seems best for consistency and
	 * future-proofing.  Likewise we disallow all digits though only octal
	 * digits are actually dangerous.
	 */
	if (!csv_mode && !delim_off &&
		strchr("\\.abcdefghijklmnopqrstuvwxyz0123456789", delim[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("delimiter cannot be \"%s\"", delim)));

	if (delim_off)
	{

		/*
		 * We don't support delimiter 'off' for COPY because the QD COPY
		 * sometimes internally adds columns to the data that it sends to
		 * the QE COPY modules, and it uses the delimiter for it. There
		 * are ways to work around this but for now it's not important and
		 * we simply don't support it.
		 */
		if (copy)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("Using no delimiter is only supported for external tables")));

		if (num_columns != 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("Using no delimiter is only possible for a single column table")));

	}

	/*
	 * HEADER
	 */
	if(header_line)
	{
		if(!copy && Gp_role == GP_ROLE_DISPATCH)
		{
			/* (exttab) */
			if(load)
			{
				/* RET */
				ereport(NOTICE,
						(errmsg("HEADER means that each one of the data files "
								"has a header row.")));				
			}
			else
			{
				/* WET */
				ereport(ERROR,
						(errcode(ERRCODE_GP_FEATURE_NOT_YET),
						errmsg("HEADER is not yet supported for writable external tables")));				
			}
		}
	}

	/*
	 * QUOTE
	 */
	if (!csv_mode && quote != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("quote available only in CSV mode"),
				errOmitLocation(true)));

	if (csv_mode && strlen(quote) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("quote must be a single character"),
				errOmitLocation(true)));

	if (csv_mode && strchr(null_print, quote[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("CSV quote character must not appear in the NULL specification")));

	if (csv_mode && delim[0] == quote[0])
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("delimiter and quote must be different")));

	/*
	 * ESCAPE
	 */
	if (csv_mode && strlen(escape) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("escape in CSV format must be a single character"),
			errOmitLocation(true)));

	if (!csv_mode &&
		(strchr(escape, '\r') != NULL ||
		strchr(escape, '\n') != NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("escape representation in text format cannot use newline or carriage return")));

	if (!csv_mode && strlen(escape) != 1)
	{
		if (pg_strcasecmp(escape, "off"))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("escape must be a single character, or [OFF/off] to disable escapes")));
	}

	/*
	 * FORCE QUOTE
	 */
	if (!csv_mode && force_quote != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("force quote available only in CSV mode")));
	if (force_quote != NIL && load)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("force quote only available for data unloading, not loading")));

	/*
	 * FORCE NOT NULL
	 */
	if (!csv_mode && force_notnull != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("force not null available only in CSV mode")));
	if (force_notnull != NIL && !load)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("force not null only available for data loading, not unloading")));

	if (fill_missing && !load)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("fill missing fields only available for data loading, not unloading")));

	/*
	 * NEWLINE
	 */
	if (newline)
	{
		if (!load)
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_YET),
					errmsg("newline currently available for data loading only, not unloading")));
		}
		else
		{
			if(pg_strcasecmp(newline, "lf") != 0 &&
			   pg_strcasecmp(newline, "cr") != 0 &&
			   pg_strcasecmp(newline, "crlf") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("invalid value for NEWLINE (%s)", newline),
						errhint("valid options are: 'LF', 'CRLF', 'CR'")));
		}
	}
}

/*
 *	 DoCopy executes the SQL COPY statement
 *
 * Either unload or reload contents of table <relation>, depending on <from>.
 * (<from> = TRUE means we are inserting into the table.) In the "TO" case
 * we also support copying the output of an arbitrary SELECT query.
 *
 * If <pipe> is false, transfer is between the table and the file named
 * <filename>.	Otherwise, transfer is between the table and our regular
 * input/output stream. The latter could be either stdin/stdout or a
 * socket, depending on whether we're running under Postmaster control.
 *
 * Iff <oids>, unload or reload the format that includes OID information.
 * On input, we accept OIDs whether or not the table has an OID column,
 * but silently drop them if it does not.  On output, we report an error
 * if the user asks for OIDs in a table that has none (not providing an
 * OID column might seem friendlier, but could seriously confuse programs).
 *
 * If in the text format, delimit columns with delimiter <delim> and print
 * NULL values as <null_print>.
 *
 * When loading in the text format from an input stream (as opposed to
 * a file), recognize a "\." on a line by itself as EOF. Also recognize
 * a stream EOF.  When unloading in the text format to an output stream,
 * write a "." on a line by itself at the end of the data.
 *
 * Do not allow a Postgres user without superuser privilege to read from
 * or write to a file.
 *
 * Do not allow the copy if user doesn't have proper permission to access
 * the table.
 */
uint64
DoCopy(const CopyStmt *stmt, const char *queryString)
{
	CopyState	cstate;
	bool		is_from = stmt->is_from;
	bool		pipe = (stmt->filename == NULL || Gp_role == GP_ROLE_EXECUTE);
	List	   *attnamelist = stmt->attlist;
	List	   *force_quote = NIL;
	List	   *force_notnull = NIL;
	List *err_segnos = NIL;
	AclMode		required_access = (is_from ? ACL_INSERT : ACL_SELECT);
	AclResult	aclresult;
	ListCell   *option;
	TupleDesc   tupDesc;
	int         num_phys_attrs;
	uint64      processed;
	bool        qe_copy_from = (is_from && (Gp_role == GP_ROLE_EXECUTE));
    /* save relationOid for auto-stats */
	Oid         relationOid = InvalidOid;
	int savedSegNum = -1;

	/* Allocate workspace and zero all fields */
	cstate = (CopyStateData *) palloc0(sizeof(CopyStateData));

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "binary") == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("BINARY is not supported")));
		}
		else if (strcmp(defel->defname, "oids") == 0)
		{
			if (cstate->oids)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->oids = intVal(defel->arg);
		}
		else if (strcmp(defel->defname, "delimiter") == 0)
		{
			if (cstate->delim)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->delim = strVal(defel->arg);
		}
		else if (strcmp(defel->defname, "null") == 0)
		{
			if (cstate->null_print)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->null_print = strVal(defel->arg);

			/*
			 * MPP-2010: unfortunately serialization function doesn't
			 * distinguish between 0x0 and empty string. Therefore we
			 * must assume that if NULL AS was indicated and has no value
			 * the actual value is an empty string.
			 */
			if(!cstate->null_print)
				cstate->null_print = "";
		}
		else if (strcmp(defel->defname, "csv") == 0)
		{
			if (cstate->csv_mode)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->csv_mode = intVal(defel->arg);
		}
		else if (strcmp(defel->defname, "header") == 0)
		{
			if (cstate->header_line)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->header_line = intVal(defel->arg);
		}
		else if (strcmp(defel->defname, "quote") == 0)
		{
			if (cstate->quote)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->quote = strVal(defel->arg);
		}
		else if (strcmp(defel->defname, "escape") == 0)
		{
			if (cstate->escape)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->escape = strVal(defel->arg);
		}
		else if (strcmp(defel->defname, "force_quote") == 0)
		{
			if (force_quote)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			force_quote = (List *) defel->arg;
		}
		else if (strcmp(defel->defname, "force_notnull") == 0)
		{
			if (force_notnull)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			force_notnull = (List *) defel->arg;
		}
		else if (strcmp(defel->defname, "fill_missing_fields") == 0)
		{
			if (cstate->fill_missing)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->fill_missing = intVal(defel->arg);
		}
		else if (strcmp(defel->defname, "newline") == 0)
		{
			if (cstate->eol_str)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->eol_str = strVal(defel->arg);
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	/* Set defaults */

	cstate->err_loc_type = ROWNUM_ORIGINAL;
	cstate->eol_type = EOL_UNKNOWN;
	cstate->escape_off = false;

	if (!cstate->delim)
		cstate->delim = cstate->csv_mode ? "," : "\t";

	if (!cstate->null_print)
		cstate->null_print = cstate->csv_mode ? "" : "\\N";

	if (cstate->csv_mode)
	{
		if (!cstate->quote)
			cstate->quote = "\"";
		if (!cstate->escape)
			cstate->escape = cstate->quote;
	}

	if (!cstate->csv_mode && !cstate->escape)
		cstate->escape = "\\";			/* default escape for text mode */

	/*
	 * Error handling setup
	 */
	if(stmt->sreh)
	{
		/* Single row error handling requested */
		SingleRowErrorDesc *sreh;
		sreh = (SingleRowErrorDesc *)stmt->sreh;

		if (!is_from)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY single row error handling only available using COPY FROM")));

		if (sreh->errtable)
		{
			cstate->errMode = SREH_LOG;
		}
		else
		{
			cstate->errMode = SREH_IGNORE;
			if (sreh->is_keep)
				ereport(ERROR,
						(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
						errmsg("KEEP may only be specified with a LOG INTO errortable clause")));

		}
		cstate->cdbsreh = makeCdbSreh(sreh->is_keep,
									  sreh->reusing_existing_errtable,
									  sreh->rejectlimit,
									  sreh->is_limit_in_rows,
									  sreh->errtable,
									  NULL, /* assign a value later */
									  stmt->filename,
									  stmt->relation->relname);

		/* if necessary warn the user of the risk of table getting dropped */
		if(sreh->errtable && Gp_role == GP_ROLE_DISPATCH && !sreh->reusing_existing_errtable)
			emitSameTxnWarning();
	}
	else
	{
		/* No single row error handling requested. Use "all or nothing" */
		cstate->cdbsreh = NULL; /* default - no SREH */
		cstate->errMode = ALL_OR_NOTHING; /* default */
	}

	/* We must be a QE if we received the partitioning config */
	if (stmt->partitions)
	{
		Assert(Gp_role == GP_ROLE_EXECUTE);
		cstate->partitions = stmt->partitions;
	}

	/*
	 * Validate our control characters and their combination
	 */
	ValidateControlChars(true,
						 is_from,
						 cstate->csv_mode,
						 cstate->delim,
						 cstate->null_print,
						 cstate->quote,
						 cstate->escape,
						 force_quote,
						 force_notnull,
						 cstate->header_line,
						 cstate->fill_missing,
						 cstate->eol_str,
						 0 /* pass correct value when COPY supports no delim */);

	if (!pg_strcasecmp(cstate->escape, "off"))
		cstate->escape_off = true;

	/* set end of line type if NEWLINE keyword was specified */
	if (cstate->eol_str)
		CopyEolStrToType(cstate);
	
	/* Disallow file COPY except to superusers. */
	if (!pipe && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to COPY to or from a file"),
				 errhint("Anyone can COPY to stdout or from stdin. "
						 "psql's \\copy command also works for anyone.")));

	cstate->copy_dest = COPY_FILE;		/* default */
	cstate->filename = (Gp_role == GP_ROLE_EXECUTE ? NULL : stmt->filename); /* QE COPY always uses STDIN */
	cstate->copy_file = NULL;
	cstate->fe_msgbuf = NULL;
	cstate->fe_eof = false;
	cstate->missing_bytes = 0;
	
	if(!is_from)
	{
		if (pipe)
		{
			if (whereToSendOutput == DestRemote)
				cstate->fe_copy = true;
			else
				cstate->copy_file = stdout;
		}
		else
		{
			mode_t		oumask; /* Pre-existing umask value */
			struct stat st;

			/*
			 * Prevent write to relative path ... too easy to shoot oneself in the
			 * foot by overwriting a database file ...
			 */
			if (!is_absolute_path(cstate->filename))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("relative path not allowed for COPY to file")));

			oumask = umask((mode_t) 022);
			cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_W);
			umask(oumask);

			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for writing: %m",
								cstate->filename),
						 errOmitLocation(true)));

			// Increase buffer size to improve performance  (cmcdevitt)
			setvbuf(cstate->copy_file, NULL, _IOFBF, 393216); // 384 Kbytes

			fstat(fileno(cstate->copy_file), &st);
			if (S_ISDIR(st.st_mode))
			{
				FreeFile(cstate->copy_file);
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", cstate->filename)));
			}
		}

	}

	elog(DEBUG1,"DoCopy starting");
	if (stmt->relation)
	{
		LOCKMODE lockmode = (is_from ? RowExclusiveLock : AccessShareLock);

		Assert(!stmt->query);
		cstate->queryDesc = NULL;

		/* Open and lock the relation, using the appropriate lock type. */
		cstate->rel = heap_openrv(stmt->relation, lockmode);

		/* save relation oid for auto-stats call later */
		relationOid = RelationGetRelid(cstate->rel);

		/* Check relation permissions. */
		aclresult = pg_class_aclcheck(RelationGetRelid(cstate->rel),
									  GetUserId(),
									  required_access);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_CLASS,
						   RelationGetRelationName(cstate->rel));

		/* check read-only transaction */
		if (XactReadOnly && is_from &&
			!isTempNamespace(RelationGetNamespace(cstate->rel)))
			ereport(ERROR,
					(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
					 errmsg("transaction is read-only")));

		/* Don't allow COPY w/ OIDs to or from a table without them */
		if (cstate->oids && !cstate->rel->rd_rel->relhasoids)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("table \"%s\" does not have OIDs",
							RelationGetRelationName(cstate->rel))));

		tupDesc = RelationGetDescr(cstate->rel);
	}
	else
	{
		Query	   *query = stmt->query;
		List	   *rewritten;
		PlannedStmt	 *plannedstmt;
		DestReceiver *dest;

		Assert(query);
		Assert(!is_from);
		cstate->rel = NULL;

		/* Don't allow COPY w/ OIDs from a select */
		if (cstate->oids)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY (SELECT) WITH OIDS is not supported"),
					 errOmitLocation(true)));

		if (query->intoClause)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY (SELECT INTO) is not supported"),
					 errOmitLocation(true)));


		/*
		 * The query has already been through parse analysis, but not
		 * rewriting or planning.  Do that now.
		 *
		 * Because the planner is not cool about not scribbling on its input,
		 * we make a preliminary copy of the source querytree.  This prevents
		 * problems in the case that the COPY is in a portal or plpgsql
		 * function and is executed repeatedly.  (See also the same hack in
												  * EXPLAIN, DECLARE CURSOR and PREPARE.)  XXX the planner really
		 * shouldn't modify its input ... FIXME someday.
		 */
		query = copyObject(query);
		Assert(query->commandType == CMD_SELECT);

		/*
		 * Must acquire locks in case we didn't come fresh from the parser.
		 * XXX this also scribbles on query, another reason for copyObject
		 */
		AcquireRewriteLocks(query);

		/* Rewrite through rule system */
		rewritten = QueryRewrite(query);

		/* We don't expect more or less than one result query */
		if (list_length(rewritten) != 1)
			elog(ERROR, "unexpected rewrite result");

		query = (Query *) linitial(rewritten);
		Assert(query->commandType == CMD_SELECT);

		/* plan the query */
		plannedstmt = planner(query, 0, NULL, QRL_ONCE);

		/*
		 * Update snapshot command ID to ensure this query sees results of any
		 * previously executed queries.  (It's a bit cheesy to modify
		 * ActiveSnapshot without making a copy, but for the limited ways in
		 * which COPY can be invoked, I think it's OK, because the active
		 * snapshot shouldn't be shared with anything else anyway.)
		 */
		ActiveSnapshot->curcid = GetCurrentCommandId();

		/* Create dest receiver for COPY OUT */
		dest = CreateDestReceiver(DestCopyOut, NULL);
		((DR_copy *) dest)->cstate = cstate;

		/* Create a QueryDesc requesting no output */
		cstate->queryDesc = CreateQueryDesc(plannedstmt, queryString,
											ActiveSnapshot, InvalidSnapshot,
											dest, NULL, false);

		/*
		 * Call ExecutorStart to prepare the plan for execution.
		 *
		 * ExecutorStart computes a result tupdesc for us
		 */
		ExecutorStart(cstate->queryDesc, 0);

		tupDesc = cstate->queryDesc->tupDesc;
	}

	cstate->attnamelist = attnamelist;
	/* Generate or convert list of attributes to process */
	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

	num_phys_attrs = tupDesc->natts;

	/* Convert FORCE QUOTE name list to per-column flags, check validity */
	cstate->force_quote_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (force_quote)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, force_quote);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE QUOTE column \"%s\" not referenced by COPY",
								NameStr(tupDesc->attrs[attnum - 1]->attname))));
			cstate->force_quote_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE NOT NULL name list to per-column flags, check validity */
	cstate->force_notnull_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (force_notnull)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, force_notnull);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE NOT NULL column \"%s\" not referenced by COPY",
								NameStr(tupDesc->attrs[attnum - 1]->attname))));
			cstate->force_notnull_flags[attnum - 1] = true;
		}

		/* keep the raw version too, we will need it later */
		cstate->force_notnull = force_notnull;
	}

	/*
	 * Set up variables to avoid per-attribute overhead.
	 */
	initStringInfo(&cstate->attribute_buf);
	initStringInfo(&cstate->line_buf);
	cstate->processed = 0;

	/*
	 * Set up encoding conversion info.  Even if the client and server
	 * encodings are the same, we must apply pg_client_to_server() to
	 * validate data in multibyte encodings. However, transcoding must
	 * be skipped for COPY FROM in executor mode since data already arrived
	 * in server encoding (was validated and trancoded by dispatcher mode
	 * COPY). For this same reason encoding_embeds_ascii can never be true
	 * for COPY FROM in executor mode.
	 */
	cstate->client_encoding = pg_get_client_encoding();
	cstate->need_transcoding =
		((cstate->client_encoding != GetDatabaseEncoding() ||
		  pg_database_encoding_max_length() > 1) && !qe_copy_from);

	cstate->encoding_embeds_ascii = (qe_copy_from ? false : PG_ENCODING_IS_CLIENT_ONLY(cstate->client_encoding));
	cstate->line_buf_converted = (Gp_role == GP_ROLE_EXECUTE ? true : false);
	setEncodingConversionProc(cstate, pg_get_client_encoding(), !is_from);

	/*
	 * some greenplum db specific vars
	 */
	cstate->is_copy_in = (is_from ? true : false);
	if (is_from)
	{
		cstate->error_on_executor = false;
		initStringInfo(&(cstate->executor_err_context));
	}

	if (is_from)				/* copy from file to database */
	{
		bool		pipe = (cstate->filename == NULL);
		bool		shouldDispatch = (Gp_role == GP_ROLE_DISPATCH &&
									  cstate->rel->rd_cdbpolicy != NULL);
		char		relkind;

		Assert(cstate->rel);

		relkind = cstate->rel->rd_rel->relkind;

    if (relkind != RELKIND_RELATION)
		{
			if (relkind == RELKIND_VIEW)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot copy to view \"%s\"",
								RelationGetRelationName(cstate->rel))));
			else if (relkind == RELKIND_SEQUENCE)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot copy to sequence \"%s\"",
								RelationGetRelationName(cstate->rel))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot copy to non-table relation \"%s\"",
								RelationGetRelationName(cstate->rel))));
		}

		if(stmt->sreh && Gp_role != GP_ROLE_EXECUTE && !cstate->rel->rd_cdbpolicy)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY single row error handling only available for distributed user tables")));

		if (pipe)
		{
			if (whereToSendOutput == DestRemote)
				ReceiveCopyBegin(cstate);
			else
				cstate->copy_file = stdin;
		}
		else
		{
			struct stat st;

			cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_R);

			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for reading: %m",
								cstate->filename),
						 errOmitLocation(true)));

			// Increase buffer size to improve performance  (cmcdevitt)
            setvbuf(cstate->copy_file, NULL, _IOFBF, 393216); // 384 Kbytes

			fstat(fileno(cstate->copy_file), &st);
			if (S_ISDIR(st.st_mode))
			{
				FreeFile(cstate->copy_file);
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", cstate->filename)));
			}
		}


		/*
		 * Append Only Tables.
		 *
		 * If QD, build a list of all the relations (relids) that may get data
		 * inserted into them as a part of this operation. This includes
		 * the relation specified in the COPY command, plus any partitions
		 * that it may have. Then, call assignPerRelSegno to assign a segfile
		 * number to insert into each of the Append Only relations that exists
		 * in this global list. We generate the list now and save it in cstate.
		 *
		 * If QE - get the QD generated list from CopyStmt and each relation can
		 * find it's assigned segno by looking at it (during CopyFrom).
		 *
		 * Utility mode always builds a one single mapping.
		 */
		if(shouldDispatch)
		{
			Oid		relid = RelationGetRelid(cstate->rel);
			List	*all_relids = NIL;
			GpPolicy *target_policy = NULL;
			int target_segment_num = 0;
			QueryResource *savedResource = NULL;

			target_policy = GpPolicyFetch(CurrentMemoryContext, relid);
			Assert(target_policy);
			target_segment_num = target_policy->bucketnum;
			pfree(target_policy);

			cstate->resource = AllocateResource(QRL_ONCE, 1, 1, target_segment_num, target_segment_num,NULL,0);
			savedResource = GetActiveQueryResource();
			SetActiveQueryResource(cstate->resource);
			all_relids = lappend_oid(all_relids, relid);

			if (rel_is_partitioned(relid))
			{
				PartitionNode *pn = RelationBuildPartitionDesc(cstate->rel, false);
				all_relids = list_concat(all_relids, all_partition_relids(pn));
			}

			cstate->ao_segnos = assignPerRelSegno(all_relids, list_length(cstate->resource->segments));

			ListCell *cell;
			foreach(cell, all_relids)
			{
				Oid relid =  lfirst_oid(cell);
				CreateAppendOnlyParquetSegFileOnMaster(relid, cstate->ao_segnos);
			}

			/* allocate segno for error table */
			if (stmt->sreh && cstate->cdbsreh->errtbl)
			{
				Oid		relid = RelationGetRelid(cstate->cdbsreh->errtbl);
				Assert(!rel_is_partitioned(relid));
				err_segnos = SetSegnoForWrite(NIL, relid,  list_length(cstate->resource->segments), false, true);
				if (Gp_role == GP_ROLE_DISPATCH)
					CreateAppendOnlyParquetSegFileForRelationOnMaster(
							cstate->cdbsreh->errtbl,
							err_segnos);
			}
			SetActiveQueryResource(savedResource);
		}
		else
		{
			if (cstate->cdbsreh)
			{
				int segno = 0;
				ResultRelSegFileInfo *segfileinfo = NULL;
				if (stmt->err_aosegnos != NIL)
				{
					segno = list_nth_int(stmt->err_aosegnos, GetQEIndex());
					segfileinfo = (ResultRelSegFileInfo *)list_nth(stmt->err_aosegfileinfos, GetQEIndex());
				}
				cstate->cdbsreh->err_aosegno = segno;
				cstate->cdbsreh->err_aosegfileinfo = segfileinfo;
			}
			
			if (stmt->ao_segnos)
			{
				/* We must be a QE if we received the aosegnos config */
				Assert(Gp_role == GP_ROLE_EXECUTE);
				cstate->ao_segnos = stmt->ao_segnos;
				cstate->ao_segfileinfos = stmt->ao_segfileinfos;
			}
			else
			{
				/*
				 * utility mode (or dispatch mode for no policy table).
				 * create a one entry map for our one and only relation
				 */
				if (RelationIsAoRows(cstate->rel) || RelationIsParquet(cstate->rel))
				{
					SegfileMapNode *n = makeNode(SegfileMapNode);
					n->relid = RelationGetRelid(cstate->rel);
					n->segnos = SetSegnoForWrite(NIL, n->relid, 1, false, true);
					cstate->ao_segnos = lappend(cstate->ao_segnos, n);
				}
			}
		}

		/*
		 * Set up is done. Get to work!
		 */
		if (shouldDispatch)
		{
			/* data needs to get dispatched to segment databases */
			CopyFromDispatch(cstate, err_segnos);
		}
		else
		{
			/* data needs to get inserted locally */
			CopyFrom(cstate);
		}

		if (!pipe)
		{
			if (FreeFile(cstate->copy_file))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to file \"%s\": %m",
								cstate->filename)));
		}
	}
	else		/* copy from database to file */
	{
		if (stmt->scantable_splits)
		{
			Assert(Gp_role == GP_ROLE_EXECUTE);
			if (RelationIsAoRows(cstate->rel) || RelationIsParquet(cstate->rel))
			{
				cstate->splits = GetFileSplitsOfSegment(stmt->scantable_splits,
								cstate->rel->rd_id, GetQEIndex());
			}
			else
			{
				cstate->splits = NIL;
			}
		}
		cstate->resource = NULL;
		DoCopyTo(cstate);
	}

	/*
	 * Close the relation or query.  If reading, we can release the
	 * AccessShareLock we got; if writing, we should hold the lock until end
	 * of transaction to ensure that updates will be committed before lock is
	 * released.
	 */
	if (cstate->rel)
		heap_close(cstate->rel, (is_from ? NoLock : AccessShareLock));
	if (cstate->queryDesc)
	{
		/* Close down the query and free resources. */
	  if (Gp_role == GP_ROLE_DISPATCH && cstate->queryDesc->resource != NULL)
	  {
	    savedSegNum = list_length(cstate->queryDesc->resource->segments);
	  }
		ExecutorEnd(cstate->queryDesc);
		FreeQueryDesc(cstate->queryDesc);
	}

	/* Clean up single row error handling related memory */
	if(cstate->cdbsreh)
		destroyCdbSreh(cstate->cdbsreh);

	/* Clean up storage (probably not really necessary) */
	processed = cstate->processed;

    /* MPP-4407. Logging number of tuples copied */
	if (Gp_role == GP_ROLE_DISPATCH
			&& is_from
			&& relationOid != InvalidOid
			&& GetCommandLogLevel((Node *) stmt) <= log_statement)
	{
		elog(DEBUG1, "type_of_statement = %s dboid = %d tableoid = %d num_tuples_modified = %u",
				autostats_cmdtype_to_string(AUTOSTATS_CMDTYPE_COPY),
				MyDatabaseId,
				relationOid,
				(unsigned int) processed);
	}

    /* 	 Fix for MPP-4082. Issue automatic ANALYZE if conditions are satisfied. */
	if (Gp_role == GP_ROLE_DISPATCH && is_from)
	{
		auto_stats(AUTOSTATS_CMDTYPE_COPY, relationOid, processed, false /* inFunction */, savedSegNum);
	} /*end auto-stats block*/

	if(cstate->force_quote_flags)
		pfree(cstate->force_quote_flags);
	if(cstate->force_notnull_flags)
		pfree(cstate->force_notnull_flags);
		
	pfree(cstate->attribute_buf.data);
	pfree(cstate->line_buf.data);
	if (cstate->resource)
		FreeResource(cstate->resource);

	pfree(cstate);
	return processed;
}

/*
 * This intermediate routine exists mainly to localize the effects of setjmp
 * so we don't need to plaster a lot of variables with "volatile".
 */
static void
DoCopyTo(CopyState cstate)
{
	bool		pipe = (cstate->filename == NULL);

	if (cstate->rel)
	{
		char		relkind = cstate->rel->rd_rel->relkind;

		if (relkind != RELKIND_RELATION)
		{
			if (relkind == RELKIND_VIEW)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot copy from view \"%s\"",
								RelationGetRelationName(cstate->rel)),
						 errhint("Try the COPY (SELECT ...) TO variant."),
						 errOmitLocation(true)));
			else if (relkind == RELKIND_SEQUENCE)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot copy from sequence \"%s\"",
								RelationGetRelationName(cstate->rel)),
										 errOmitLocation(true)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot copy from non-table relation \"%s\"",
								RelationGetRelationName(cstate->rel)),
										 errOmitLocation(true)));
		}
		else if (RelationIsExternal(cstate->rel))
		{
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot copy from external relation \"%s\"",
								RelationGetRelationName(cstate->rel)),
						 errhint("Try the COPY (SELECT ...) TO variant."),
								 errOmitLocation(true)));
		}
	}

	PG_TRY();
	{
		if (cstate->fe_copy)
			SendCopyBegin(cstate);

		/*
		 * We want to dispatch COPY TO commands only in the case that
		 * we are the dispatcher and we are copying from a user relation
		 * (a relation where data is distributed in the segment databases).
		 * Otherwize, if we are not the dispatcher *or* if we are
		 * doing COPY (SELECT) we just go straight to work, without
		 * dispatching COPY commands to executors.
		 */
		if (Gp_role == GP_ROLE_DISPATCH && cstate->rel && cstate->rel->rd_cdbpolicy)
		{
			GpPolicy *target_policy = NULL;
			int target_segment_num = 0;

			target_policy = GpPolicyFetch(CurrentMemoryContext, cstate->rel->rd_id);
			Assert(target_policy);
			target_segment_num = target_policy->bucketnum;
			pfree(target_policy);

			cstate->resource = AllocateResource(QRL_ONCE, 1, 1, target_segment_num, target_segment_num,NULL,0);
			CopyToDispatch(cstate);
		}
		else
			CopyTo(cstate);

		if (cstate->fe_copy)
			SendCopyEnd(cstate);
	}
	PG_CATCH();
	{
		/*
		 * Make sure we turn off old-style COPY OUT mode upon error. It is
		 * okay to do this in all cases, since it does nothing if the mode is
		 * not on.
		 */
		pq_endcopyout(true);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!pipe)
	{
		if (FreeFile(cstate->copy_file))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m",
							cstate->filename)));
	}
}

/*
 * CopyToCreateDispatchCommand
 *
 * Create the COPY command that will get dispatched to the QE's.
 */
static void CopyToCreateDispatchCommand(CopyState cstate,
										StringInfo cdbcopy_cmd,
										AttrNumber	num_phys_attrs,
										Form_pg_attribute *attr)

{
	ListCell   *cur;
	bool		is_first_col = true;

	/* append schema and tablename */
	appendStringInfo(cdbcopy_cmd, "COPY %s.%s",
					 quote_identifier(get_namespace_name(RelationGetNamespace(cstate->rel))),
					 quote_identifier(RelationGetRelationName(cstate->rel)));
	/*
	 * append column list. NOTE: if not specified originally, attnumlist will
	 * include all non-dropped columns of the table by default
	 */
	if(num_phys_attrs > 0) /* don't append anything for zero column table */
	{
		foreach(cur, cstate->attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;

			/* We don't add dropped attributes */
			if (attr[m]->attisdropped)
				continue;

			/* append column string. quote it if needed */
			appendStringInfo(cdbcopy_cmd, (is_first_col ? "(%s" : ",%s"),
							 quote_identifier(NameStr(attr[m]->attname)));

			is_first_col = false;
		}

		if (!is_first_col)
			appendStringInfo(cdbcopy_cmd, ")");
	}

	appendStringInfo(cdbcopy_cmd, " TO STDOUT WITH");

	if (cstate->oids)
		appendStringInfo(cdbcopy_cmd, " OIDS");

	appendStringInfo(cdbcopy_cmd, " DELIMITER AS E'%s'", cstate->delim);
	appendStringInfo(cdbcopy_cmd, " NULL AS E'%s'", escape_quotes(cstate->null_print));

	/* if default escape in text format ("\") leave expression out */
	if (!cstate->csv_mode && strcmp(cstate->escape, "\\") != 0)
		appendStringInfo(cdbcopy_cmd, " ESCAPE AS E'%s'", cstate->escape);

	if (cstate->csv_mode)
	{
		appendStringInfo(cdbcopy_cmd, " CSV");
		appendStringInfo(cdbcopy_cmd, " QUOTE AS E'%s'", escape_quotes(cstate->quote));
		appendStringInfo(cdbcopy_cmd, " ESCAPE AS E'%s'", escape_quotes(cstate->escape));

		/* do NOT include HEADER. Header row is created by dispatcher COPY */
	}

}


/*
 * Copy from relation TO file. Starts a COPY TO command on each of
 * the executors and gathers all the results and writes it out.
 */
 void
CopyToDispatch(CopyState cstate)
{
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	int			attr_count;
	Form_pg_attribute *attr;
	CdbCopy    *cdbCopy;
	StringInfoData cdbcopy_err;
	StringInfoData cdbcopy_cmd;

	tupDesc = cstate->rel->rd_att;
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	/*
	 * prepare to get COPY data from segDBs:
	 * 1 - re-construct the orignial COPY command sent from the client.
	 * 2 - execute a BEGIN DTM transaction.
	 * 3 - send the COPY command to all segment databases.
	 */

	cdbCopy = makeCdbCopy(false, cstate->resource);
	
	cdbCopy->partitions = RelationBuildPartitionDesc(cstate->rel, false);

	/* XXX: lock all partitions */

	/* allocate memory for error and copy strings */
	initStringInfo(&cdbcopy_err);
	initStringInfo(&cdbcopy_cmd);

	/* create the command to send to QE's and store it in cdbcopy_cmd */
	CopyToCreateDispatchCommand(cstate,
								&cdbcopy_cmd,
								num_phys_attrs,
								attr);

	/*
	 * Start a COPY command in every db of every segment in Greenplum Database.
	 *
	 * From this point in the code we need to be extra careful
	 * about error handling. ereport() must not be called until
	 * the COPY command sessions are closed on the executors.
	 * Calling ereport() will leave the executors hanging in
	 * COPY state.
	 */
	elog(DEBUG5, "COPY command sent to segdbs: %s", cdbcopy_cmd.data);

	PG_TRY();
	{
		cdbCopyStart(cdbCopy, cdbcopy_cmd.data, RelationGetRelid(cstate->rel), InvalidOid, NIL);
	}
	PG_CATCH();
	{
		/* get error message from CopyStart */
		appendBinaryStringInfo(&cdbcopy_err, cdbCopy->err_msg.data, cdbCopy->err_msg.len);

		/* TODO: end COPY in all the segdbs in progress */
		cdbCopyEnd(cdbCopy);

		ereport(LOG,
				(errcode(ERRCODE_CDB_INTERNAL_ERROR),
				 errmsg("%s", cdbcopy_err.data)));
		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_TRY();
	{
		/* if a header has been requested send the line */
		if (cstate->header_line)
		{
			ListCell   *cur;
			bool		hdr_delim = false;

			/*
			 * For non-binary copy, we need to convert null_print to client
			 * encoding, because it will be sent directly with CopySendString.
			 *
			 * MPP: in here we only care about this if we need to print the
			 * header. We rely on the segdb server copy out to do the conversion
			 * before sending the data rows out. We don't need to repeat it here
			 */
			if (cstate->need_transcoding)
				cstate->null_print = (char *)
					pg_server_to_custom(cstate->null_print,
										strlen(cstate->null_print),
										cstate->client_encoding,
										cstate->enc_conversion_proc);

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;

				if (hdr_delim)
					CopySendChar(cstate, cstate->delim[0]);
				hdr_delim = true;

				colname = NameStr(attr[attnum - 1]->attname);

				CopyAttributeOutCSV(cstate, colname, false,
									list_length(cstate->attnumlist) == 1);
			}

			/* add a newline and flush the data */
			CopySendEndOfRow(cstate);
		}

		/*
		 * This is the main work-loop. In here we keep collecting data from the
		 * COPY commands on the segdbs, until no more data is available. We
		 * keep writing data out a chunk at a time.
		 */
		while(true)
		{

			bool done;
			bool copy_cancel = (QueryCancelPending ? true : false);

			/* get a chunk of data rows from the QE's */
			done = cdbCopyGetData(cdbCopy, copy_cancel, &cstate->processed);

			/* send the chunk of data rows to destination (file or stdout) */
			if(cdbCopy->copy_out_buf.len > 0) /* conditional is important! */
			{
				/*
				 * in the dispatcher we receive chunks of whole rows with row endings.
				 * We don't want to use CopySendEndOfRow() b/c it adds row endings and
				 * also b/c it's intended for a single row at a time. Therefore we need
				 * to fill in the out buffer and just flush it instead.
				 */
				CopySendData(cstate, (void *) cdbCopy->copy_out_buf.data, cdbCopy->copy_out_buf.len);
				CopyToDispatchFlush(cstate);
			}

			if(done)
			{
				if(cdbCopy->remote_data_err || cdbCopy->io_errors)
					appendBinaryStringInfo(&cdbcopy_err, cdbCopy->err_msg.data, cdbCopy->err_msg.len);

				break;
			}
		}
		dispatch_free_result(&cdbCopy->executors);
	}
	PG_CATCH();
	{
		dispatch_free_result(&cdbCopy->executors);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* we can throw the error now if QueryCancelPending was set previously */
	CHECK_FOR_INTERRUPTS();

	/*
	 * report all accumulated errors back to the client.
	 */
	if (cdbCopy->remote_data_err)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("%s", cdbcopy_err.data)));
	if (cdbCopy->io_errors)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("%s", cdbcopy_err.data)));

	pfree(cdbcopy_cmd.data);
	pfree(cdbcopy_err.data);
	pfree(cdbCopy);
}


/*
 * Copy from relation or query TO file.
 */
static void
CopyTo(CopyState cstate)
{
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	Form_pg_attribute *attr;
	ListCell   *cur;
	List *target_rels = NIL;
	ListCell *lc;

	if (cstate->rel)
	{
		if (cstate->partitions)
		{
			ListCell *lc;
			List *relids = all_partition_relids(cstate->partitions);

			foreach(lc, relids)
			{
				Oid relid = lfirst_oid(lc);
				Relation rel = heap_open(relid, AccessShareLock);

				target_rels = lappend(target_rels, rel);
			}
		}
		else
			target_rels = lappend(target_rels, cstate->rel);

		tupDesc = RelationGetDescr(cstate->rel);
	}
	else
		tupDesc = cstate->queryDesc->tupDesc;

	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	cstate->null_print_client = cstate->null_print;		/* default */

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	cstate->out_functions =
		(FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));

	/* Get info about the columns we need to process. */
	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;

		getTypeOutputInfo(attr[attnum - 1]->atttypid,
						  &out_func_oid,
						  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.	(We don't need a whole econtext as CopyFrom does.)
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "COPY TO",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * we need to convert null_print to client
	 * encoding, because it will be sent directly with CopySendString.
	 */
	if (cstate->need_transcoding)
		cstate->null_print_client = pg_server_to_custom(cstate->null_print,
														cstate->null_print_len,
														cstate->client_encoding,
														cstate->enc_conversion_proc);

	/* if a header has been requested send the line */
	if (cstate->header_line)
	{
		/* header should not be printed in execute mode. */
		if (Gp_role != GP_ROLE_EXECUTE)
		{
			bool		hdr_delim = false;

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;

				if (hdr_delim)
					CopySendChar(cstate, cstate->delim[0]);
				hdr_delim = true;

				colname = NameStr(attr[attnum - 1]->attname);

				CopyAttributeOutCSV(cstate, colname, false,
									list_length(cstate->attnumlist) == 1);
			}

			CopySendEndOfRow(cstate);
		}
	}

	if (cstate->rel)
	{
		foreach(lc, target_rels)
		{
			Relation rel = lfirst(lc);
			Datum	   *values;
			bool	   *nulls;
			HeapScanDesc scandesc = NULL;			/* used if heap table */
			AppendOnlyScanDesc aoscandesc = NULL;	/* append only table */

			tupDesc = RelationGetDescr(rel);
			attr = tupDesc->attrs;
			num_phys_attrs = tupDesc->natts;

			/*
			 * We need to update attnumlist because different partition
			 * entries might have dropped tables.
			 */
			cstate->attnumlist =
				CopyGetAttnums(tupDesc, rel, cstate->attnamelist);

			pfree(cstate->out_functions);
			cstate->out_functions =
				(FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));

			/* Get info about the columns we need to process. */
			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				Oid			out_func_oid;
				bool		isvarlena;

				getTypeOutputInfo(attr[attnum - 1]->atttypid,
								  &out_func_oid,
								  &isvarlena);
				fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
			}

			values = (Datum *) palloc(num_phys_attrs * sizeof(Datum));
			nulls = (bool *) palloc(num_phys_attrs * sizeof(bool));

			if (RelationIsHeap(rel))
			{
				HeapTuple	tuple;

				scandesc = heap_beginscan(rel, ActiveSnapshot, 0, NULL);
				while ((tuple = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
				{
					CHECK_FOR_INTERRUPTS();

					/* Deconstruct the tuple ... faster than repeated heap_getattr */
					heap_deform_tuple(tuple, tupDesc, values, nulls);

					/* Format and send the data */
					CopyOneRowTo(cstate, HeapTupleGetOid(tuple), values, nulls);
				}

				heap_endscan(scandesc);
			}
			else if(RelationIsAoRows(rel))
			{
				MemTuple		tuple;
				TupleTableSlot	*slot = MakeSingleTupleTableSlot(tupDesc);
				MemTupleBinding *mt_bind = create_memtuple_binding(tupDesc);

				aoscandesc = appendonly_beginscan(rel, ActiveSnapshot, 0, NULL);
				aoscandesc->splits = cstate->splits;

				while ((tuple = appendonly_getnext(aoscandesc, ForwardScanDirection, slot)) != NULL)
				{
					CHECK_FOR_INTERRUPTS();

					/* Extract all the values of the  tuple */
					slot_getallattrs(slot);
					values = slot_get_values(slot);
					nulls = slot_get_isnull(slot);

					/* Format and send the data */
					CopyOneRowTo(cstate, MemTupleGetOid(tuple, mt_bind), values, nulls);
				}

				ExecDropSingleTupleTableSlot(slot);

				appendonly_endscan(aoscandesc);
			}
			else if(RelationIsParquet(rel))
			{
				ParquetScanDesc scan = NULL;
				TupleTableSlot	*slot = MakeSingleTupleTableSlot(tupDesc);

				bool *proj = NULL;

				int nvp = tupDesc->natts;
				int i;

				if (tupDesc->tdhasoid)
				{
					elog(ERROR, "OIDS=TRUE is not allowed on tables that use column-oriented storage. Use OIDS=FALSE");
				}

				proj = palloc(sizeof(bool) * nvp);
				for(i=0; i<nvp; ++i)
					proj[i] = true;

				scan = parquet_beginscan(rel, ActiveSnapshot, 0, proj);
				scan->splits = cstate->splits;
				for(;;)
				{
					parquet_getnext(scan, ForwardScanDirection, slot);
					if (TupIsNull(slot))
					    break;

					/* Extract all the values of the  tuple */
					slot_getallattrs(slot);
					values = slot_get_values(slot);
					nulls = slot_get_isnull(slot);

					/* Format and send the data */
					CopyOneRowTo(cstate, InvalidOid, values, nulls);
				}

				ExecDropSingleTupleTableSlot(slot);

				parquet_endscan(scan);
			}
			else
			{
				/* should never get here */
				Assert(false);
			}

			/* partition table, so close */
			if (cstate->partitions)
				heap_close(rel, NoLock);
		}
	}
	else
	{
		Assert(Gp_role != GP_ROLE_EXECUTE);

		/* run the plan --- the dest receiver will send tuples */
		ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0L);
	}

	MemoryContextDelete(cstate->rowcontext);
}

void
CopyOneCustomRowTo(CopyState cstate, bytea *value)
{	
	appendBinaryStringInfo(cstate->fe_msgbuf, 
						   VARDATA_ANY((void *) value), 
						   VARSIZE_ANY_EXHDR((void *) value));
}

/*
 * Emit one row during CopyTo().
 */
void
CopyOneRowTo(CopyState cstate, Oid tupleOid, Datum *values, bool *isnulls)
{
	bool		need_delim = false;
	FmgrInfo   *out_functions = cstate->out_functions;
	MemoryContext oldcontext;
	ListCell   *cur;
	char	   *string;

	MemoryContextReset(cstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(cstate->rowcontext);

	/* Text format has no per-tuple header, but send OID if wanted */
	/* Assume digits don't need any quoting or encoding conversion */
	if (cstate->oids)
	{
		string = DatumGetCString(DirectFunctionCall1(oidout,
													 ObjectIdGetDatum(tupleOid)));
		CopySendString(cstate, string);
		need_delim = true;
	}

	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);

		bool		isnull = isnulls[attnum-1];
		Datum		value = values[attnum-1];

		if (need_delim)
			CopySendChar(cstate, cstate->delim[0]);

		need_delim = true;

		if (isnull)
		{
			CopySendString(cstate, cstate->null_print_client);
		}
		else
		{
			/* int2out or int4out ? */
			if (out_functions[attnum -1].fn_oid == 39 ||  /* int2out or int4out */
				out_functions[attnum -1].fn_oid == 43 )
			{
				char tmp[33];
				/*
				 * The standard postgres way is to call the output function, but that involves one or more pallocs,
				 * and a call to sprintf, followed by a conversion to client charset.
				 * Do a fast conversion to string instead.
				 */

				if (out_functions[attnum -1].fn_oid ==  39)
					pg_itoa(DatumGetInt16(value),tmp);
				else
					pg_ltoa(DatumGetInt32(value),tmp);

				/*
				 * integers shouldn't need quoting, and shouldn't need transcoding to client char set
				 */
				CopySendData(cstate, tmp, strlen(tmp));
			}
			else if (out_functions[attnum -1].fn_oid == 1702)   /* numeric_out */
			{
				string = OutputFunctionCall(&out_functions[attnum - 1],
																value);
				/*
				 * numeric shouldn't need quoting, and shouldn't need transcoding to client char set
				 */
				CopySendData(cstate, string, strlen(string));
			}
			else
			{
				string = OutputFunctionCall(&out_functions[attnum - 1],
											value);
				if (cstate->csv_mode)
					CopyAttributeOutCSV(cstate, string,
										cstate->force_quote_flags[attnum - 1],
										list_length(cstate->attnumlist) == 1);
				else
					CopyAttributeOutText(cstate, string);
			}
		}
	}

	/*
	 * Finish off the row: write it to the destination, and update the count.
	 * However, if we're in the context of a writable external table, we let 
	 * the caller do it - send the data to its local external source (see
	 * external_insert() ).
	 */
	if(cstate->copy_dest != COPY_EXTERNAL_SOURCE)
	{
		CopySendEndOfRow(cstate);
		cstate->processed++;
	}
	
	MemoryContextSwitchTo(oldcontext);
}


/*
 * CopyFromCreateDispatchCommand
 *
 * The COPY command that needs to get dispatched to the QE's isn't necessarily
 * the same command that arrived from the parser to the QD. For example, we
 * always change filename to STDIN, we may pre-evaluate constant values or
 * functions on the QD and send them to the QE with an extended column list.
 */
static void CopyFromCreateDispatchCommand(CopyState cstate,
										  StringInfo cdbcopy_cmd,
										  GpPolicy  *policy,
										  AttrNumber	num_phys_attrs,
										  AttrNumber	num_defaults,
										  AttrNumber	p_nattrs,
										  AttrNumber	h_attnum,
										  int *defmap,
										  ExprState **defexprs,
										  Form_pg_attribute *attr)
{
	ListCell   *cur;
	bool		is_first_col;
	int			i,
				p_index = 0;
	AttrNumber	extra_attr_count = 0; /* count extra attributes we add in the dispatcher COPY
										 usually non constant defaults we pre-evaluate in here */

	Assert(Gp_role == GP_ROLE_DISPATCH);

	/* append schema and tablename */
	appendStringInfo(cdbcopy_cmd, "COPY %s.%s",
					 quote_identifier(get_namespace_name(RelationGetNamespace(cstate->rel))),
					 quote_identifier(RelationGetRelationName(cstate->rel)));
	/*
	 * append column list. NOTE: if not specified originally, attnumlist will
	 * include all non-dropped columns of the table by default
	 */
	if(num_phys_attrs > 0) /* don't append anything for zero column table */
	{
		is_first_col = true;
		foreach(cur, cstate->attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;

			/* We don't add dropped attributes */
			if (attr[m]->attisdropped)
				continue;

			/* append column string. quote it if needed */
			appendStringInfo(cdbcopy_cmd, (is_first_col ? "(%s" : ",%s"),
							 quote_identifier(NameStr(attr[m]->attname)));

			is_first_col = false;
		}

		/*
		 * In order to maintain consistency between the primary and mirror segment data, we
		 * want to evaluate all table columns that are not participating in this COPY command
		 * and have a non-constant default values on the dispatcher. If we let them evaluate
		 * on the primary and mirror executors separately - they will get different values.
		 * Also, if the distribution column is not participating and it has any default value,
		 * we have to evaluate it on the dispatcher only too, so that it wouldn't hash as a null
		 * and inserted as a default value on the segment databases.
		 *
		 * Therefore, we include these columns in the column list for the executor COPY.
		 * The default values will be evaluated on the dispatcher COPY and the results for
		 * the added columns will be appended to each data row that is shipped to the segments.
		 */
		extra_attr_count = 0;

		for (i = 0; i < num_defaults; i++)
		{
			bool add_to_list = false;

			/* check 1: is this default for a distribution column? */
			for (p_index = 0; p_index < p_nattrs; p_index++)
			{
				h_attnum = policy->attrs[p_index];

				if(h_attnum - 1 == defmap[i])
					add_to_list = true;
			}

			/* check 2: is this a non constant default? */
			if(defexprs[i]->expr->type != T_Const)
				add_to_list = true;

			if(add_to_list)
			{
				/* We don't add dropped attributes */
				if (attr[defmap[i]]->attisdropped)
					continue;

				/* append column string. quote it if needed */
				appendStringInfo(cdbcopy_cmd, (is_first_col ? "(%s" : ",%s"),
								 quote_identifier(NameStr(attr[defmap[i]]->attname)));

				extra_attr_count++;
				is_first_col = false;
			}
		}

		if (!is_first_col)
			appendStringInfo(cdbcopy_cmd, ")");
	}

	/*
	 * NOTE: we used to always pass STDIN here to the QEs. But since we want
	 * the QEs to know the original file name for recording it in an error table
	 * (if they use one) we actually pass the filename here, and in the QE COPY
	 * we get it, save it, and then always revert back to actually using STDIN.
	 * (if we originally use STDIN we just pass it along and record that in the
	 * error table).
	 */
	if(cstate->filename)
		appendStringInfo(cdbcopy_cmd, " FROM '%s' WITH", cstate->filename);
	else
		appendStringInfo(cdbcopy_cmd, " FROM STDIN WITH");

	if (cstate->oids)
		appendStringInfo(cdbcopy_cmd, " OIDS");

	appendStringInfo(cdbcopy_cmd, " DELIMITER AS E'%s'", cstate->delim);
	appendStringInfo(cdbcopy_cmd, " NULL AS E'%s'", escape_quotes(cstate->null_print));

	/* if default escape in text format ("\") leave expression out */
	if (!cstate->csv_mode && strcmp(cstate->escape, "\\") != 0)
		appendStringInfo(cdbcopy_cmd, " ESCAPE AS E'%s'", cstate->escape);

	/* if EOL is already defined it means that NEWLINE was declared. pass it along */
	if (cstate->eol_type != EOL_UNKNOWN)
	{
		Assert(cstate->eol_str);
		appendStringInfo(cdbcopy_cmd, " NEWLINE AS '%s'", cstate->eol_str);
	}
		
	if (cstate->csv_mode)
	{
		appendStringInfo(cdbcopy_cmd, " CSV");
		appendStringInfo(cdbcopy_cmd, " QUOTE AS E'%s'", escape_quotes(cstate->quote));
		appendStringInfo(cdbcopy_cmd, " ESCAPE AS E'%s'", escape_quotes(cstate->escape));

		if(cstate->force_notnull)
		{
			ListCell   *l;

			is_first_col = true;
			appendStringInfo(cdbcopy_cmd, " FORCE NOT NULL");

			foreach(l, cstate->force_notnull)
			{
				const char	   *col_name = strVal(lfirst(l));

				appendStringInfo(cdbcopy_cmd, (is_first_col ? " %s" : ",%s"),
								 quote_identifier(col_name));
				is_first_col = false;
			}
		}
		/* do NOT include HEADER. Header row is "swallowed" by dispatcher COPY */
	}

	if (cstate->fill_missing)
		appendStringInfo(cdbcopy_cmd, " FILL MISSING FIELDS");

	/* add single row error handling clauses if necessary */
	if (cstate->errMode != ALL_OR_NOTHING)
	{
		if (cstate->errMode == SREH_LOG)
		{
			appendStringInfo(cdbcopy_cmd, " LOG ERRORS INTO %s.%s",
							 quote_identifier(get_namespace_name(RelationGetNamespace(cstate->cdbsreh->errtbl))),
							 quote_identifier(RelationGetRelationName(cstate->cdbsreh->errtbl)));
		}

		appendStringInfo(cdbcopy_cmd, " SEGMENT REJECT LIMIT %d %s",
						 cstate->cdbsreh->rejectlimit, (cstate->cdbsreh->is_limit_in_rows ? "ROWS" : "PERCENT"));
	}

}

/*
 * Copy FROM file to relation.
 */
void
CopyFromDispatch(CopyState cstate, List *err_segnos)
{
	TupleDesc	tupDesc;
	Form_pg_attribute *attr;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults;
	FmgrInfo   *in_functions;
	FmgrInfo   *out_functions; /* for handling defaults in Greenplum Database */
	Oid		   *typioparams;
	int			attnum;
	int			i;
	int			p_index;
	Oid			in_func_oid;
	Oid			out_func_oid;
	Oid         relerror = InvalidOid;
	Datum	   *values;
	bool	   *nulls;
	int		   *attr_offsets;
	int			total_rejeted_from_qes = 0;
	bool		isnull;
	bool	   *isvarlena;
	ResultRelInfo *resultRelInfo;
	EState	   *estate = CreateExecutorState(); /* for ExecConstraints() */
	bool		file_has_oids = false;
	int		   *defmap;
	ExprState **defexprs;		/* array of default att expressions */
	ExprContext *econtext;		/* used for ExecEvalExpr for default atts */
	MemoryContext oldcontext = CurrentMemoryContext;
	ErrorContextCallback errcontext;
	bool		no_more_data = false;
	ListCell   *cur;
	bool		cur_row_rejected = false;
	CdbCopy    *cdbCopy;

	/*
	 * This stringInfo will contain 2 types of error messages:
	 *
	 * 1) Data errors refer to errors that are a result of inappropriate
	 *	  input data or constraint violations. All data error messages
	 *	  from the segment databases will be added to this variable and
	 *	  reported back to the client at the end of the copy command
	 *	  execution on the dispatcher.
	 * 2) Any command execution error that occurs during this COPY session.
	 *	  Such errors will usually be failure to send data over the network,
	 *	  a COPY command that was rejected by the segment databases or any I/O
	 *	  error.
	 */
	StringInfoData cdbcopy_err;

	/*
	 * a reconstructed and modified COPY command that is dispatched to segments.
	 */
	StringInfoData cdbcopy_cmd;

	/*
	 * Variables for cdbpolicy
	 */
	GpPolicy  *policy; /* the partitioning policy for this table */
	AttrNumber	p_nattrs; /* num of attributes in the distribution policy */
	Oid       *p_attr_types;	/* types for each policy attribute */

	/* variables for partitioning */
	Datum      *part_values = NULL;
	Oid		   *part_attr_types = NULL; /* types for partitioning */
	Oid		   *part_typio = NULL;
	FmgrInfo   *part_infuncs = NULL;
	AttrNumber *part_attnum = NULL;
	int			part_attnums = 0;

	/*
	 * Variables for original row number tracking
	 */
	StringInfoData line_buf_with_lineno;
	int			original_lineno_for_qe;

	/*
	 * Variables for cdbhash
	 */

	/*
	 * In the case of partitioned tables with children that have different
	 * distribution policies, we maintain a hash table of CdbHashs and
	 * GpPolicies for each child table. We lazily add them to the hash --
	 * when a partition is returned which we haven't seen before, we makeCdbHash
	 * and copy the policy over.
	 */
	typedef struct
	{
		Oid			relid;
		CdbHash    *cdbHash;		/* a CdbHash API object		 */
		GpPolicy   *policy;			/* policy for this cdb hash */
	} cdbhashdata;

	/* The actually hash table. Only initialised if we need it. */
	HTAB *hashmap = NULL;

	CdbHash *cdbHash = NULL;
	AttrNumber	h_attnum;		/* hash key attribute number */
	Datum		h_key;			/* hash key value			 */
	unsigned int target_seg = 0;	/* result segment of cdbhash */

	tupDesc = RelationGetDescr(cstate->rel);
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);
	num_defaults = 0;
	h_attnum = 0;

	/*
	 * Init original row number tracking vars
	 */
	initStringInfo(&line_buf_with_lineno);
	original_lineno_for_qe = 1;

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of
	 * code here that basically duplicated execUtils.c ...)
	 */
	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
	resultRelInfo->ri_RelationDesc = cstate->rel;
	resultRelInfo->ri_TrigDesc = CopyTriggerDesc(cstate->rel->trigdesc);
	if (resultRelInfo->ri_TrigDesc)
		resultRelInfo->ri_TrigFunctions = (FmgrInfo *)
            palloc0(resultRelInfo->ri_TrigDesc->numtriggers * sizeof(FmgrInfo));
    resultRelInfo->ri_TrigInstrument = NULL;
    ResultRelInfoSetSegno(resultRelInfo, cstate->ao_segnos);

	ExecOpenIndices(resultRelInfo);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	econtext = GetPerTupleExprContext(estate);

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass
	 * to the input function), and info about defaults and constraints.
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));
	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));
	isvarlena = (bool *) palloc(num_phys_attrs * sizeof(bool));


	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		/* We don't need info for dropped attributes */
		if (attr[attnum - 1]->attisdropped)
			continue;

		/* Fetch the input function and typioparam info */
		getTypeInputInfo(attr[attnum - 1]->atttypid,
						 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);

		/*
		 * Fetch the output function and typioparam info. We need it
		 * for handling default functions on the dispatcher COPY, if
		 * there are any.
		 */
		getTypeOutputInfo(attr[attnum - 1]->atttypid,
						  &out_func_oid,
						  &isvarlena[attnum - 1]);
		fmgr_info(out_func_oid, &out_functions[attnum - 1]);

		/* TODO: is force quote array necessary for default conversion */

		/* Get default info if needed */
		if (!list_member_int(cstate->attnumlist, attnum))
		{
			/* attribute is NOT to be copied from input */
			/* use default value if one exists */
			Node	   *defexpr = build_column_default(cstate->rel, attnum);

			if (defexpr != NULL)
			{
				defexprs[num_defaults] = ExecPrepareExpr((Expr *) defexpr,
														 estate);
				defmap[num_defaults] = attnum - 1;
				num_defaults++;
			}
		}
	}

	/*
	 * prepare to COPY data into segDBs:
	 * - set table partitioning information
	 * - set append only table relevant info for dispatch.
	 * - get the distribution policy for this table.
	 * - build a COPY command to dispatch to segdbs.
	 * - dispatch the modified COPY command to all segment databases.
	 * - prepare cdbhash for hashing on row values.
	 */
	cdbCopy = makeCdbCopy(true, cstate->resource);

	estate->es_result_partitions = cdbCopy->partitions =
		RelationBuildPartitionDesc(cstate->rel, false);

	CopyInitPartitioningState(estate);

	if (list_length(cstate->ao_segnos) > 0)
		cdbCopy->ao_segnos = cstate->ao_segnos;

	/* add cdbCopy reference to cdbSreh (if needed) */
	if (cstate->errMode != ALL_OR_NOTHING)
		cstate->cdbsreh->cdbcopy = cdbCopy;

	/* get the CDB policy for this table and prepare for hashing */
	if (estate->es_result_partitions &&
		!partition_policies_equal(cstate->rel->rd_cdbpolicy,
								  estate->es_result_partitions))
	{
		/*
		 * This is a partitioned table that has multiple, different
		 * distribution policies.
		 *
		 * We build up a fake policy comprising the set of all columns used
		 * to distribute all children in the partition configuration. That way
		 * we're sure to parse all necessary columns in the input data and we
		 * have all column types handy.
		 */
		List *cols = NIL;
		ListCell *lc;
		HASHCTL hash_ctl;

		partition_get_policies_attrs(estate->es_result_partitions,
									 cstate->rel->rd_cdbpolicy,
									 &cols);
        MemSet(&hash_ctl, 0, sizeof(hash_ctl));
        hash_ctl.keysize = sizeof(Oid);
        hash_ctl.entrysize = sizeof(cdbhashdata);
        hash_ctl.hash = oid_hash;
        hash_ctl.hcxt = CurrentMemoryContext;

        hashmap = hash_create("partition cdb hash map",
                              100 /* XXX: need a better value, but what? */,
                              &hash_ctl,
                              HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
		p_nattrs = list_length(cols);
		policy = palloc(sizeof(GpPolicy) + sizeof(AttrNumber) * p_nattrs);
		i = 0;
		foreach(lc, cols)
			policy->attrs[i++] = lfirst_int(lc);

	}
	else
	{
		policy = GpPolicyCopy(CurrentMemoryContext, cstate->rel->rd_cdbpolicy);

		if (policy)
			p_nattrs = policy->nattrs;	/* number of partitioning keys */
		else
			p_nattrs = 0;
		/* Create hash API reference */
		cdbHash = makeCdbHash(cdbCopy->partition_num, HASH_FNV_1);
	}


	/*
	 * Extract types for each partition key from the tuple descriptor,
	 * and convert them when necessary. We don't want to do this
	 * for each tuple since get_typtype() is quite expensive when called
	 * lots of times.
	 *
	 * The array key for p_attr_types is the attribute number of the attribute
	 * in question.
	 */
	p_attr_types = (Oid *)palloc0(num_phys_attrs * sizeof(Oid));
	for (i = 0; i < p_nattrs; i++)
	{
		h_attnum = policy->attrs[i];

		/*
		 * get the data type of this attribute. If it's an
		 * array type use anyarray, or else just use as is.
		 */
		if (attr[h_attnum - 1]->attndims > 0)
			p_attr_types[h_attnum - 1] = ANYARRAYOID;
		else
		{
			/* If this type is a domain type, get its base type. */
			p_attr_types[h_attnum - 1] = attr[h_attnum - 1]->atttypid;
			if (get_typtype(p_attr_types[h_attnum - 1]) == 'd')
			    p_attr_types[h_attnum - 1] =
					getBaseType(p_attr_types[h_attnum - 1]);
		}
	}

	/* allocate memory for error and copy strings */
	initStringInfo(&cdbcopy_err);
	initStringInfo(&cdbcopy_cmd);

	/* store the COPY command string in cdbcopy_cmd */
	CopyFromCreateDispatchCommand(cstate,
								  &cdbcopy_cmd,
								  policy,
								  num_phys_attrs,
								  num_defaults,
								  p_nattrs,
								  h_attnum,
								  defmap,
								  defexprs,
								  attr);

	/*
	 * for optimized parsing - get the last field number in the
	 * file that we need to parse to have all values for the hash keys.
	 * (If the table has an empty distribution policy, then we don't need
	 * to parse any attributes really... just send the row away using
	 * a special cdbhash function designed for this purpose).
	 */
	cstate->last_hash_field = 0;

	for (p_index = 0; p_index < p_nattrs; p_index++)
	{
		i = 1;

		/*
		 * for this partitioning key, search for its location in the attr list.
		 * (note that fields may be out of order).
		 */
		foreach(cur, cstate->attnumlist)
		{
			int			attnum = lfirst_int(cur);

			if (attnum == policy->attrs[p_index])
			{
				if (i > cstate->last_hash_field)
					cstate->last_hash_field = i;
			}

			if (estate->es_result_partitions)
			{
				if (attnum == estate->es_partition_state->max_partition_attr)
				{
					if (i > cstate->last_hash_field)
						cstate->last_hash_field = i;
				}				
			}

			i++;
		}
	}

	/*
	 * Dispatch the COPY command.
	 *
	 * From this point in the code we need to be extra careful about error
	 * handling. ereport() must not be called until the COPY command sessions
	 * are closed on the executors. Calling ereport() will leave the executors
	 * hanging in COPY state.
	 *
	 * For errors detected by the dispatcher, we save the error message in
	 * cdbcopy_err StringInfo, move on to closing all COPY sessions on the
	 * executors and only then raise an error. We need to make sure to TRY/CATCH
	 * all other errors that may be raised from elsewhere in the backend. All
	 * error during COPY on the executors will be detected only when we end the
	 * COPY session there, so we are fine there.
	 */
	elog(DEBUG5, "COPY command sent to segdbs: %s", cdbcopy_cmd.data);
	PG_TRY();
	{
	    if (cstate->cdbsreh && cstate->cdbsreh->errtbl)
	        relerror = RelationGetRelid(cstate->cdbsreh->errtbl);

		cdbCopyStart(cdbCopy, cdbcopy_cmd.data,
				RelationGetRelid(cstate->rel), relerror,
				err_segnos);
	}
	PG_CATCH();
	{
		/* get error message from CopyStart */
		appendBinaryStringInfo(&cdbcopy_err, cdbCopy->err_msg.data, cdbCopy->err_msg.len);

		/* end COPY in all the segdbs in progress */
		cdbCopyEnd(cdbCopy);

		/* get error message from CopyEnd */
		appendBinaryStringInfo(&cdbcopy_err, cdbCopy->err_msg.data, cdbCopy->err_msg.len);

		ereport(LOG,
				(errcode(ERRCODE_CDB_INTERNAL_ERROR),
				 errmsg("%s", cdbcopy_err.data)));
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Prepare to catch AFTER triggers.
	 */
	//AfterTriggerBeginQuery();

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debateable whether
	 * we should do this for COPY, since it's not really an "INSERT"
	 * statement as such. However, executing these triggers maintains
	 * consistency with the EACH ROW triggers that we already fire on
	 * COPY.
	 */
	//ExecBSInsertTriggers(estate, resultRelInfo);

	file_has_oids = cstate->oids;	/* must rely on user to tell us this... */

	values = (Datum *) palloc(num_phys_attrs * sizeof(Datum));
	nulls = (bool *) palloc(num_phys_attrs * sizeof(bool));
	attr_offsets = (int *) palloc(num_phys_attrs * sizeof(int));

	/* Set up callback to identify error line number */
	errcontext.callback = copy_in_error_callback;
	errcontext.arg = (void *) cstate;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;
	cstate->err_loc_type = ROWNUM_ORIGINAL;

	CopyInitDataParser(cstate);

	do
	{
		size_t		bytesread = 0;

		/* read a chunk of data into the buffer */
		PG_TRY();
		{
			bytesread = CopyGetData(cstate, cstate->raw_buf, RAW_BUF_SIZE);
		}
		PG_CATCH();
		{
			/*
			 * If we are here, we got some kind of communication error
			 * with the client or a bad protocol message. clean up and
			 * re-throw error. Note that we don't handle this error in
			 * any special way in SREH mode as it's not a data error.
			 */
			cdbCopyEnd(cdbCopy);
			PG_RE_THROW();
		}
		PG_END_TRY();

		cstate->raw_buf_done = false;

		/* set buffer pointers to beginning of the buffer */
		cstate->begloc = cstate->raw_buf;
		cstate->raw_buf_index = 0;

		/*
		 * continue if some bytes were read or if we didn't reach EOF. if we
		 * both reached EOF _and_ no bytes were read, quit the loop we are
		 * done
		 */
		if (bytesread > 0 || !cstate->fe_eof)
		{
			/* on first time around just throw the header line away */
			if (cstate->header_line)
			{
				PG_TRY();
				{
					cstate->line_done = cstate->csv_mode ?
						CopyReadLineCSV(cstate, bytesread) :
						CopyReadLineText(cstate, bytesread);
				}
				PG_CATCH();
				{
					/*
					 * TODO: use COPY_HANDLE_ERROR here, but make sure to
					 * ignore this error per the "note:" below.
					 */

					/*
					 * got here? encoding conversion error occured on the
					 * header line (first row).
					 */
					if(cstate->errMode == ALL_OR_NOTHING)
					{
						/* re-throw error and abort */
						cdbCopyEnd(cdbCopy);
						PG_RE_THROW();
					}
					else
					{
						/* SREH - release error state */
						if(!elog_dismiss(DEBUG5))
							PG_RE_THROW(); /* hope to never get here! */

						/*
						 * note: we don't bother doing anything special here.
						 * we are never interested in logging a header line
						 * error. just continue the workflow.
						 */
					}
				}
				PG_END_TRY();

				cstate->cur_lineno++;
				RESET_LINEBUF;

				cstate->header_line = false;
			}

			while (!cstate->raw_buf_done)
			{
				Oid			loaded_oid = InvalidOid;
				GpPolicy   *part_policy = NULL; /* policy for specific part */
				AttrNumber	part_p_nattrs = 0; /* partition policy max attno */
				CdbHash	   *part_hash = NULL; /* hash for the part policy */

				if (QueryCancelPending)
				{
					/* quit processing loop */
					no_more_data = true;
					break;
				}

				/* Reset the per-tuple exprcontext */
				ResetPerTupleExprContext(estate);

				/* Switch into its memory context */
				MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

				/* Initialize all values for row to NULL */
				MemSet(values, 0, num_phys_attrs * sizeof(Datum));
				MemSet(nulls, true, num_phys_attrs * sizeof(bool));
				MemSet(attr_offsets, 0, num_phys_attrs * sizeof(int));

				/* Get the line number of the first line of this data row */
				original_lineno_for_qe = cstate->cur_lineno + 1;

				PG_TRY();
				{
					/* Actually read the line into memory here */
					cstate->line_done = cstate->csv_mode ?
						CopyReadLineCSV(cstate, bytesread) :
						CopyReadLineText(cstate, bytesread);
				}
				PG_CATCH();
				{
					/* got here? encoding conversion/check error occurred */
					COPY_HANDLE_ERROR;
				}
				PG_END_TRY();

				if(cur_row_rejected)
				{
					IF_REJECT_LIMIT_REACHED_ABORT;
					QD_GOTO_NEXT_ROW;
				}


				if(!cstate->line_done)
				{
					/*
					 * We did not finish reading a complete data line.
					 *
					 * If eof is not yet reached, we skip att parsing
					 * and read more data. But if eof _was_ reached it means
					 * that the original last data line is defective and
					 * we want to catch that error later on.
					 */
					if (!cstate->fe_eof || cstate->end_marker)
						break;
				}

				if (file_has_oids)
				{
					char	   *oid_string;

					/* can't be in CSV mode here */
					oid_string = CopyReadOidAttr(cstate, &isnull);

					if (isnull)
					{
						/* got here? null in OID column error */

						if(cstate->errMode == ALL_OR_NOTHING)
						{
							/* report error and abort */
							cdbCopyEnd(cdbCopy);

							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("null OID in COPY data.")));
						}
						else
						{
							/* SREH */
							cstate->cdbsreh->rejectcount++;
							cur_row_rejected = true;
						}

					}
					else
					{
						PG_TRY();
						{
							cstate->cur_attname = "oid";
							loaded_oid = DatumGetObjectId(DirectFunctionCall1(oidin,
										   CStringGetDatum(oid_string)));
						}
						PG_CATCH();
						{
							/* got here? oid column conversion failed */
							COPY_HANDLE_ERROR;
						}
						PG_END_TRY();

						if (loaded_oid == InvalidOid)
						{
							if(cstate->errMode == ALL_OR_NOTHING)
							{
								/* report error and abort */
								cdbCopyEnd(cdbCopy);

								ereport(ERROR,
										(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
										 errmsg("invalid OID in COPY data.")));
							}
							else
							{
								/* SREH */
								cstate->cdbsreh->rejectcount++;
								cur_row_rejected = true;
							}
						}

						cstate->cur_attname = NULL;
					}

					if(cur_row_rejected)
					{
						IF_REJECT_LIMIT_REACHED_ABORT
						QD_GOTO_NEXT_ROW;
					}
				}


				PG_TRY();
				{
					/*
					 * parse and convert the data line attributes.
					 */
					if (cstate->csv_mode)
						CopyReadAttributesCSV(cstate, nulls, attr_offsets, num_phys_attrs, attr);
					else
						CopyReadAttributesText(cstate, nulls, attr_offsets, num_phys_attrs, attr);

					attr_get_key(cstate, cdbCopy,
								 original_lineno_for_qe,
								 target_seg,
								 p_nattrs, policy->attrs,
								 attr, attr_offsets, nulls,
							   	 in_functions, typioparams,
								 values);

					/*
					 * Now compute defaults for only:
					 * 1 - the distribution column,
					 * 2 - any other column with a non-constant default expression
					 * (such as a function) that is, of course, if these columns
					 * not provided by the input data.
					 * Anything not processed here or above will remain NULL.
					 */
					for (i = 0; i < num_defaults; i++)
					{
						bool compute_default = false;

						/* check 1: is this default for a distribution column? */
						for (p_index = 0; p_index < p_nattrs; p_index++)
						{
							h_attnum = policy->attrs[p_index];

							if(h_attnum - 1 == defmap[i])
								compute_default = true;
						}

						/* check 2: is this a default function? (non-constant default) */
						if(defexprs[i]->expr->type != T_Const)
							compute_default = true;

						if(compute_default)
						{
							char *string;

							values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
															 &isnull, NULL);

							/*
							 * prepare to concatinate next value:
							 * remove eol characters from end of line buf
							 */
							truncateEol(&cstate->line_buf, cstate->eol_type);

							if (isnull)
							{
								appendStringInfo(&cstate->line_buf, "%c%s", cstate->delim[0], cstate->null_print);
							}
							else
							{
								nulls[defmap[i]] = false;

								appendStringInfo(&cstate->line_buf, "%c", cstate->delim[0]); /* write the delimiter */

								string = DatumGetCString(FunctionCall3(&out_functions[defmap[i]],
																	   values[defmap[i]],
																	   ObjectIdGetDatum(typioparams[defmap[i]]),
																	   Int32GetDatum(attr[defmap[i]]->atttypmod)));
								if (cstate->csv_mode)
								{
									CopyAttributeOutCSV(cstate, string,
														false, /*force_quote[attnum - 1],*/
														list_length(cstate->attnumlist) == 1);
								}
								else
									CopyAttributeOutText(cstate, string);
							}

							/* re-add the eol characters */
							concatenateEol(cstate);
						}

					}

					/* lock partition */
					if (estate->es_result_partitions)
					{
						PartitionNode *n = estate->es_result_partitions;
						MemoryContext cxt_save;
						/* lazily initialised */
						if (part_values == NULL)
						{
							List *pattnums = get_partition_attrs(n);
							ListCell *lc;
							int ii = 0;

							cxt_save = MemoryContextSwitchTo(oldcontext);

							part_values = palloc0(num_phys_attrs * sizeof(Datum));
							part_attr_types = palloc(num_phys_attrs * sizeof(Oid));
							part_typio = palloc(num_phys_attrs * sizeof(Oid));
							part_infuncs =
								palloc(num_phys_attrs * sizeof(FmgrInfo));
							part_attnum = palloc(num_phys_attrs *
												 sizeof(AttrNumber));
							part_attnums = list_length(pattnums);
							MemoryContextSwitchTo(cxt_save);

							foreach(lc, pattnums)
							{
								AttrNumber attnum = (AttrNumber)lfirst_int(lc);
								Oid in_func_oid;

								getTypeInputInfo(attr[attnum - 1]->atttypid,
												 &in_func_oid,
												 &part_typio[attnum - 1]);
								fmgr_info(in_func_oid, &part_infuncs[attnum - 1]);
								part_attnum[ii++] = attnum;
							}
						}
						MemSet(part_values, 0, num_phys_attrs * sizeof(Datum));

						attr_get_key(cstate, cdbCopy,
									 original_lineno_for_qe,
									 target_seg,
									 part_attnums,
									 part_attnum,
									 attr, attr_offsets, nulls,
									 part_infuncs, part_typio,
									 part_values);

						/* values_get_partition() calls palloc() */
						cxt_save = MemoryContextSwitchTo(oldcontext);

						resultRelInfo = values_get_partition(part_values,
															 nulls,
															 tupDesc, estate);

						MemoryContextSwitchTo(cxt_save);

						/*
						 * If we a partition set with differing policies,
						 * get the policy for this particular child partition.
						 */
						if (hashmap)
						{
							bool found;
							cdbhashdata *d;
							Oid relid = resultRelInfo->ri_RelationDesc->rd_id;

							d = hash_search(hashmap, &(relid), HASH_ENTER,
											&found);
							if (found)
							{
								part_policy = d->policy;
								part_p_nattrs = part_policy->nattrs;
								part_hash = d->cdbHash;
							}
							else
							{
								Relation rel = heap_open(relid, NoLock);
								MemoryContext save_cxt;

								/*
								 * Make sure this all persists the current
								 * iteration.
								 */
								save_cxt = MemoryContextSwitchTo(oldcontext);
								d->relid = relid;
								part_hash = d->cdbHash =
									makeCdbHash(cdbCopy->partition_num, HASH_FNV_1);
								part_policy = d->policy =
									GpPolicyCopy(oldcontext,
												 rel->rd_cdbpolicy);
								part_p_nattrs = part_policy->nattrs;
								heap_close(rel, NoLock);
								MemoryContextSwitchTo(save_cxt);
							}
						}
					}

					/*
					 * The the usual case or a partitioned table
					 * with non-divergent child table policies.
					 */
					if (!part_hash)
					{
						part_hash = cdbHash;
						part_policy = policy;
						part_p_nattrs = p_nattrs;
					}

					/*
					 * policy should be PARTITIONED (normal tables) or
					 * ENTRY
					 */
					if (!part_policy || part_policy->ptype == POLICYTYPE_UNDEFINED)
					{
						elog(FATAL, "Bad or undefined policy. (%p)", part_policy);
					}
				}
				PG_CATCH();
				{
					COPY_HANDLE_ERROR;
				}
				PG_END_TRY();

				if(cur_row_rejected)
				{
					IF_REJECT_LIMIT_REACHED_ABORT;
					QD_GOTO_NEXT_ROW;
				}

				/*
				 * At this point in the code, values[x] is final for this
				 * data row -- either the input data, a null or a default
				 * value is in there, and constraints applied.
				 *
				 * Perform a cdbhash on this data row. Perform a hash operation
				 * on each attribute that is included in CDB policy (partitioning
				 * key columns). Send COPY data line to the target segment
				 * database executors. Data row will not be inserted locally.
				 */
				Assert(PointerIsValid(part_hash));
				cdbhashinit(part_hash);

				for (i = 0; i < part_p_nattrs; i++)
				{
					/* current attno from the policy */
					h_attnum = part_policy->attrs[i];

					h_key = values[h_attnum - 1];	/* value of this attr */

					if (!nulls[h_attnum - 1])
						cdbhash(part_hash, h_key, p_attr_types[h_attnum - 1]);
					else
						cdbhashnull(part_hash);
				}

				/*
				 * If this is a relation with an empty policy, there is no
				 * hash key to use, therefore use cdbhashnokey() to pick a
				 * hash value for us.
				 */
				if (part_p_nattrs == 0)
					cdbhashnokey(part_hash);

				target_seg = cdbhashreduce(part_hash);	/* hash result segment */


				/*
				 * Send data row to all databases for this segment.
				 * Also send the original row number with the data.
				 * modify the data to look like:
				 *    "<lineno>^<linebuf_converted>^<data>"
				 */
				appendStringInfo(&line_buf_with_lineno, "%d%c%d%c%s",
								 original_lineno_for_qe,
								 COPY_METADATA_DELIM,
								 cstate->line_buf_converted, \
								 COPY_METADATA_DELIM, \
								 cstate->line_buf.data);
				
				/* send modified data */
				cdbCopySendData(cdbCopy,
								target_seg,
								line_buf_with_lineno.data,
								line_buf_with_lineno.len);

				RESET_LINEBUF_WITH_LINENO;

				cstate->processed++;
				if (estate->es_result_partitions)
					resultRelInfo->ri_aoprocessed++;

				if (cdbCopy->io_errors)
				{
					appendBinaryStringInfo(&cdbcopy_err, cdbCopy->err_msg.data, cdbCopy->err_msg.len);
					no_more_data = true;
					break;
				}

				RESET_LINEBUF;

			}					/* end while(!raw_buf_done) */
		}						/* end if (bytesread > 0 || !cstate->fe_eof) */
		else
			/* no bytes read, end of data */
		{
			no_more_data = true;
		}
	} while (!no_more_data);

	/* Free p_attr_types */
	pfree(p_attr_types);

	/*
	 * Done reading input data and sending it off to the segment
	 * databases Now we would like to end the copy command on
	 * all segment databases across the cluster.
	 */
	total_rejeted_from_qes = cdbCopyEnd(cdbCopy);

	/*
	 * If we quit the processing loop earlier due to a
	 * cancel query signal, we now throw an error.
	 * (Safe to do only after cdbCopyEnd).
	 */
	CHECK_FOR_INTERRUPTS();


	if (cdbCopy->remote_data_err || cdbCopy->io_errors)
		appendBinaryStringInfo(&cdbcopy_err, cdbCopy->err_msg.data, cdbCopy->err_msg.len);

	if (cdbCopy->remote_data_err)
	{
		cstate->error_on_executor = true;
		if(cdbCopy->err_context.len > 0)
			appendBinaryStringInfo(&cstate->executor_err_context, cdbCopy->err_context.data, cdbCopy->err_context.len);
	}

	/*
	 * report all accumulated errors back to the client. We get here if an error
	 * happened in all-or-nothing error handling mode or if reject limit was
	 * reached in single-row error handling mode.
	 */
	if (cdbCopy->remote_data_err)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("%s", cdbcopy_err.data),
				 errOmitLocation(true)));
	if (cdbCopy->io_errors)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("%s", cdbcopy_err.data),
				 errOmitLocation(true)));

	/*
	 * switch back away from COPY error context callback. don't want line
	 * error information anymore
	 */
	error_context_stack = errcontext.previous;

	/*
	 * If we got here it means that either all the data was loaded or some rows
	 * were rejected in SREH mode. In other words - nothing caused an abort.
	 * We now want to report the actual number of rows loaded and rejected.
	 * If any rows were rejected from the QE COPY processes subtract this number
	 * from the number of rows that were successfully processed on the QD COPY
	 * so that we can report the correct number.
	 */
	if(cstate->cdbsreh)
	{
		int total_rejected = 0;
		int total_rejected_from_qd = cstate->cdbsreh->rejectcount;
		
		/* if used errtable, QD bad rows were sent to QEs and counted there. ignore QD count */
		if (cstate->cdbsreh->errtbl)
			total_rejected_from_qd = 0;
		
		total_rejected = total_rejected_from_qd + total_rejeted_from_qes;
		cstate->processed -= total_rejected;

		/* emit a NOTICE with number of rejected rows */
		ReportSrehResults(cstate->cdbsreh, total_rejected);

		/* See if we want to DROP error table when destroying cdbsreh */
		if(cstate->cdbsreh->errtbl)
			SetErrorTableVerdict(cstate->cdbsreh, total_rejected);
	}


	/*
	 * Done, clean up
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Execute AFTER STATEMENT insertion triggers
	 */
	//ExecASInsertTriggers(estate, resultRelInfo);

	/*
	 * Handle queued AFTER triggers
	 */
	//AfterTriggerEndQuery(estate);

	resultRelInfo = estate->es_result_relations;
	for (i = estate->es_num_result_relations; i > 0; i--)
	{
		/* update AO tuple counts */
		char relstorage = RelinfoGetStorage(resultRelInfo);
		if (relstorage_is_ao(relstorage))
		{
			if (cdbCopy->aotupcounts)
			{
				HTAB *ht = cdbCopy->aotupcounts;
				struct {
					Oid relid;
					int64 tupcount;
				} *ao;
				bool found;
				Oid relid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

				ao = hash_search(ht, &relid, HASH_FIND, &found);
				if (found)
				{
   	 				/* find out which segnos the result rels in the QE's used */
    			    		ResultRelInfoSetSegno(resultRelInfo, cstate->ao_segnos);

    			    		/* The total tuple count would be not necessary in HAWQ 2.0 */
    			    		/*
					UpdateMasterAosegTotals(resultRelInfo->ri_RelationDesc,
										resultRelInfo->ri_aosegno,
										ao->tupcount);
					*/
				}
			}
			else
			{
				ResultRelInfoSetSegno(resultRelInfo, cstate->ao_segnos);

				/* The total tuple count would be not necessary in HAWQ 2.0 */
				/*
				UpdateMasterAosegTotals(resultRelInfo->ri_RelationDesc,
									resultRelInfo->ri_aosegno,
									cstate->processed);
				*/
			}
		}

		/* Close indices and then the relation itself */
		ExecCloseIndices(resultRelInfo);
		heap_close(resultRelInfo->ri_RelationDesc, NoLock);
		resultRelInfo++;
	}

	/*
	 * free all resources besides ones that are needed for error reporting
	 */
	if (cdbHash)
		pfree(cdbHash);
	pfree(values);
	pfree(nulls);
	pfree(attr_offsets);
	pfree(in_functions);
	pfree(out_functions);
	pfree(isvarlena);
	pfree(typioparams);
	pfree(defmap);
	pfree(defexprs);
	pfree(cdbcopy_cmd.data);
	pfree(cdbcopy_err.data);
	pfree(line_buf_with_lineno.data);
	pfree(cdbCopy);

	if (policy)
		pfree(policy);

	/* free the hash table allocated by values_get_partition(), if any */
	if(estate->es_result_partitions && estate->es_partition_state->result_partition_hash != NULL)
		hash_destroy(estate->es_partition_state->result_partition_hash);

	/*
	 * Don't worry about the partition table hash map, that will be
	 * freed when our current memory context is freed. And that will be
	 * quite soon.
	 */
	
	cstate->rel = NULL; /* closed above */
	FreeExecutorState(estate);
}

/*
 * Copy FROM file to relation.
 */
static void
CopyFrom(CopyState cstate)
{
	void		*tuple;
	TupleDesc	tupDesc;
	Form_pg_attribute *attr;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	int			attnum;
	int			i;
	Oid			in_func_oid;
	Datum	   *values;
	bool	   *nulls;
	bool		isnull;
	ResultRelInfo *resultRelInfo;
	EState	   *estate = CreateExecutorState(); /* for ExecConstraints() */
	TupleTableSlot *slot;
	bool		file_has_oids;
	int		   *defmap;
	ExprState **defexprs;		/* array of default att expressions */
	ExprContext *econtext;		/* used for ExecEvalExpr for default atts */
	MemoryContext oldcontext = CurrentMemoryContext;
	ErrorContextCallback errcontext;
	int		   *attr_offsets;
	bool		no_more_data = false;
	ListCell   *cur;
	bool		cur_row_rejected = false;
	int			original_lineno_for_qe = 0; /* keep compiler happy (var referenced by macro) */
	CdbCopy    *cdbCopy = NULL; /* never used... for compiling COPY_HANDLE_ERROR */
	tupDesc = RelationGetDescr(cstate->rel);
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);
	num_defaults = 0;

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of
	 * code here that basically duplicated execUtils.c ...)
	 */
	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
	resultRelInfo->ri_RelationDesc = cstate->rel;
	resultRelInfo->ri_TrigDesc = CopyTriggerDesc(cstate->rel->trigdesc);
	if (resultRelInfo->ri_TrigDesc)
		resultRelInfo->ri_TrigFunctions = (FmgrInfo *)
                        palloc0(resultRelInfo->ri_TrigDesc->numtriggers * sizeof(FmgrInfo));
        resultRelInfo->ri_TrigInstrument = NULL;
        ResultRelInfoSetSegno(resultRelInfo, cstate->ao_segnos);

        ExecOpenIndices(resultRelInfo);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
	estate->es_result_partitions = cstate->partitions;

	CopyInitPartitioningState(estate);

	/* Set up a tuple slot too */
	slot = MakeSingleTupleTableSlot(tupDesc);

	econtext = GetPerTupleExprContext(estate);

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass
	 * to the input function), and info about defaults and constraints.
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));
	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		/* We don't need info for dropped attributes */
		if (attr[attnum - 1]->attisdropped)
			continue;

		/* Fetch the input function and typioparam info */
		getTypeInputInfo(attr[attnum - 1]->atttypid,
						 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);

		/* Get default info if needed */
		if (!list_member_int(cstate->attnumlist, attnum))
		{
			/* attribute is NOT to be copied from input */
			/* use default value if one exists */
			Node	   *defexpr = build_column_default(cstate->rel, attnum);

			if (defexpr != NULL)
			{
				defexprs[num_defaults] = ExecPrepareExpr((Expr *) defexpr,
														 estate);
				defmap[num_defaults] = attnum - 1;
				num_defaults++;
			}
		}

	}

	/*
	 * Prepare to catch AFTER triggers.
	 */
	AfterTriggerBeginQuery();

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debateable whether
	 * we should do this for COPY, since it's not really an "INSERT"
	 * statement as such. However, executing these triggers maintains
	 * consistency with the EACH ROW triggers that we already fire on
	 * COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	file_has_oids = cstate->oids;	/* must rely on user to tell us this... */

	values = (Datum *) palloc(num_phys_attrs * sizeof(Datum));
	nulls = (bool *) palloc(num_phys_attrs * sizeof(bool));
	attr_offsets = (int *) palloc(num_phys_attrs * sizeof(int));

	/* Set up callback to identify error line number */
	errcontext.callback = copy_in_error_callback;
	errcontext.arg = (void *) cstate;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	if (Gp_role == GP_ROLE_EXECUTE)
		cstate->err_loc_type = ROWNUM_EMBEDDED; /* get original row num from QD COPY */
	else
		cstate->err_loc_type = ROWNUM_ORIGINAL; /* we can count rows by ourselves */

	CopyInitDataParser(cstate);

	do
	{
		size_t		bytesread = 0;

		/* read a chunk of data into the buffer */
		bytesread = CopyGetData(cstate, cstate->raw_buf, RAW_BUF_SIZE);
		cstate->raw_buf_done = false;

		/* set buffer pointers to beginning of the buffer */
		cstate->begloc = cstate->raw_buf;
		cstate->raw_buf_index = 0;

		/*
		 * continue if some bytes were read or if we didn't reach EOF. if we
		 * both reached EOF _and_ no bytes were read, quit the loop we are
		 * done
		 */
		if (bytesread > 0 || !cstate->fe_eof)
		{
			/* handle HEADER, but only if we're in utility mode */
			if (cstate->header_line)
			{
				cstate->line_done = cstate->csv_mode ?
					CopyReadLineCSV(cstate, bytesread) :
					CopyReadLineText(cstate, bytesread);
				cstate->cur_lineno++;
				cstate->header_line = false;

				RESET_LINEBUF;
			}

			while (!cstate->raw_buf_done)
			{
				bool		skip_tuple;
				Oid			loaded_oid = InvalidOid;
				char		relstorage;
				
				CHECK_FOR_INTERRUPTS();

				/* Reset the per-tuple exprcontext */
				ResetPerTupleExprContext(estate);

				/* Switch into its memory context */
				MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

				/* Initialize all values for row to NULL */
				MemSet(values, 0, num_phys_attrs * sizeof(Datum));
				MemSet(nulls, true, num_phys_attrs * sizeof(bool));
				/* reset attribute pointers */
				MemSet(attr_offsets, 0, num_phys_attrs * sizeof(int));

				PG_TRY();
				{
					/* Actually read the line into memory here */
					cstate->line_done = cstate->csv_mode ?
					CopyReadLineCSV(cstate, bytesread) :
					CopyReadLineText(cstate, bytesread);
				}
				PG_CATCH();
				{
					/* got here? encoding conversion/check error occurred */
					COPY_HANDLE_ERROR;
				}
				PG_END_TRY();

				if(cur_row_rejected)
				{
					IF_REJECT_LIMIT_REACHED_ABORT;
					QE_GOTO_NEXT_ROW;
				}

				if(!cstate->line_done)
				{
					/*
					 * We did not finish reading a complete date line
					 *
					 * If eof is not yet reached, we skip att parsing
					 * and read more data. But if eof _was_ reached it means
					 * that the original last data line is defective and
					 * we want to catch that error later on.
					 */
					if (!cstate->fe_eof || cstate->end_marker)
						break;
				}

				if (file_has_oids)
				{
					char	   *oid_string;

					/* can't be in CSV mode here */
					oid_string = CopyReadOidAttr(cstate, &isnull);

					if (isnull)
					{
						/* got here? null in OID column error */

						if(cstate->errMode == ALL_OR_NOTHING)
						{
							/* report error and abort */
							cdbCopyEnd(cdbCopy);

							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("null OID in COPY data.")));
						}
						else
						{
							/* SREH */
							cstate->cdbsreh->rejectcount++;
							cur_row_rejected = true;
						}

					}
					else
					{
						PG_TRY();
						{
							cstate->cur_attname = "oid";
							loaded_oid = DatumGetObjectId(DirectFunctionCall1(oidin,
																			  CStringGetDatum(oid_string)));
						}
						PG_CATCH();
						{
							/* got here? oid column conversion failed */
							COPY_HANDLE_ERROR;
						}
						PG_END_TRY();

						if (loaded_oid == InvalidOid)
						{
							if(cstate->errMode == ALL_OR_NOTHING)
								ereport(ERROR,
										(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
										 errmsg("invalid OID in COPY data.")));
							else /* SREH */
							{
								cstate->cdbsreh->rejectcount++;
								cur_row_rejected = true;
							}
						}
						cstate->cur_attname = NULL;
					}

					if(cur_row_rejected)
					{
						IF_REJECT_LIMIT_REACHED_ABORT;
						QE_GOTO_NEXT_ROW;
					}

				}

				PG_TRY();
				{
					if (cstate->csv_mode)
						CopyReadAttributesCSV(cstate, nulls, attr_offsets, num_phys_attrs, attr);
					else
						CopyReadAttributesText(cstate, nulls, attr_offsets, num_phys_attrs, attr);

					/*
					 * Loop to read the user attributes on the line.
					 */
					foreach(cur, cstate->attnumlist)
					{
						int			attnum = lfirst_int(cur);
						int			m = attnum - 1;
						char	   *string;

						string = cstate->attribute_buf.data + attr_offsets[m];

						if (nulls[m])
							isnull = true;
						else
							isnull = false;

						if (cstate->csv_mode && isnull && cstate->force_notnull_flags[m])
						{
							string = cstate->null_print;		/* set to NULL string */
							isnull = false;
						}

						/* we read an SQL NULL, no need to do anything */
						if (!isnull)
						{
							cstate->cur_attname = NameStr(attr[m]->attname);

							values[m] = InputFunctionCall(&in_functions[m],
														  string,
														  typioparams[m],
														  attr[m]->atttypmod);

							nulls[m] = false;
							cstate->cur_attname = NULL;
						}
					}

					/*
					 * Now compute and insert any defaults available for the columns
					 * not provided by the input data.	Anything not processed here or
					 * above will remain NULL.
					 */
					for (i = 0; i < num_defaults; i++)
					{
						values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
														 &isnull, NULL);

						if (!isnull)
							nulls[defmap[i]] = false;
					}
				}
				PG_CATCH();
				{
					COPY_HANDLE_ERROR; /* SREH */
				}
				PG_END_TRY();

				if(cur_row_rejected)
				{
					IF_REJECT_LIMIT_REACHED_ABORT;
					QE_GOTO_NEXT_ROW;
				}

				/*
				 * We might create a ResultRelInfo which needs to persist
				 * the per tuple context.
				 */
				PG_TRY();
				{
					MemoryContextSwitchTo(oldcontext);
					if (estate->es_result_partitions)
					{
						resultRelInfo = values_get_partition(values, nulls,
															 tupDesc, estate);
						estate->es_result_relation_info = resultRelInfo;
					}
				}
				PG_CATCH();
				{
					COPY_HANDLE_ERROR;
				}
				PG_END_TRY();

				if (cur_row_rejected)
				{
					MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
					IF_REJECT_LIMIT_REACHED_ABORT;
					QE_GOTO_NEXT_ROW;
				}

				relstorage = RelinfoGetStorage(resultRelInfo);
				if (relstorage == RELSTORAGE_AOROWS &&
					resultRelInfo->ri_aoInsertDesc == NULL)
				{
					ResultRelSegFileInfo *segfileinfo = NULL;
					ResultRelInfoSetSegFileInfo(resultRelInfo, cstate->ao_segfileinfos);
					segfileinfo = (ResultRelSegFileInfo *)list_nth(resultRelInfo->ri_aosegfileinfos, GetQEIndex());
					resultRelInfo->ri_aoInsertDesc =
						appendonly_insert_init(resultRelInfo->ri_RelationDesc,
											   segfileinfo);
				}
				else if (relstorage == RELSTORAGE_PARQUET &&
						resultRelInfo->ri_parquetInsertDesc == NULL)
				{
					ResultRelSegFileInfo *segfileinfo = NULL;
					ResultRelInfoSetSegFileInfo(resultRelInfo, cstate->ao_segfileinfos);
					segfileinfo = (ResultRelSegFileInfo *)list_nth(resultRelInfo->ri_aosegfileinfos, GetQEIndex());
					resultRelInfo->ri_parquetInsertDesc =
						parquet_insert_init(resultRelInfo->ri_RelationDesc,
										 segfileinfo);
				}
				else if (relstorage == RELSTORAGE_EXTERNAL &&
						 resultRelInfo->ri_extInsertDesc == NULL)
				{
					resultRelInfo->ri_extInsertDesc =
						external_insert_init(resultRelInfo->ri_RelationDesc, 0);
				}

				MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

				/*
				 * And now we can form the input tuple.
				 */
				if (relstorage == RELSTORAGE_AOROWS)
				{
					/* form a mem tuple */
					tuple = (MemTuple)
						memtuple_form_to(resultRelInfo->ri_aoInsertDesc->mt_bind,
														values, nulls,
														NULL, NULL, false);

					if (cstate->oids && file_has_oids)
						MemTupleSetOid(tuple, resultRelInfo->ri_aoInsertDesc->mt_bind, loaded_oid);
				}
				else if ((relstorage == RELSTORAGE_PARQUET))
				{
                    tuple = NULL;
				}
				else
				{
					/* form a regular heap tuple */
					tuple = (HeapTuple) heap_form_tuple(tupDesc, values, nulls);

					if (cstate->oids && file_has_oids)
						HeapTupleSetOid((HeapTuple)tuple, loaded_oid);
				}


				/*
				 * Triggers and stuff need to be invoked in query context.
				 */
				MemoryContextSwitchTo(oldcontext);

				/* Partitions don't support triggers yet */
				Assert(!(estate->es_result_partitions &&
						 resultRelInfo->ri_TrigDesc));

				skip_tuple = false;

				/* BEFORE ROW INSERT Triggers */
				if (resultRelInfo->ri_TrigDesc &&
					resultRelInfo->ri_TrigDesc->n_before_row[TRIGGER_EVENT_INSERT] > 0)
				{
					HeapTuple	newtuple;

					if((relstorage == RELSTORAGE_PARQUET))
					{
						Assert(!tuple);
						elog(ERROR, "triggers are not supported on tables that use column-oriented storage");
					}

					Assert(resultRelInfo->ri_TrigFunctions != NULL);
					newtuple = ExecBRInsertTriggers(estate, resultRelInfo, tuple);

					if (newtuple == NULL)		/* "do nothing" */
						skip_tuple = true;
					else if (newtuple != tuple) /* modified by Trigger(s) */
					{
						heap_freetuple(tuple);
						tuple = newtuple;
					}
				}

				if (!skip_tuple)
				{
					char relstorage = RelinfoGetStorage(resultRelInfo);
					
					if ((relstorage != RELSTORAGE_PARQUET))
					{
						/* Place tuple in tuple slot */
						ExecStoreGenericTuple(tuple, slot, false);
					}

					else
					{
						ExecClearTuple(slot);
						slot->PRIVATE_tts_values = values;
						slot->PRIVATE_tts_isnull = nulls;
						ExecStoreVirtualTuple(slot);
					}

					/*
					 * Check the constraints of the tuple
					 */
					if (resultRelInfo->ri_RelationDesc->rd_att->constr)
							ExecConstraints(resultRelInfo, slot, estate);

					/*
					 * OK, store the tuple and create index entries for it
					 */
					if (relstorage == RELSTORAGE_AOROWS)
					{
						Oid			tupleOid;
						AOTupleId	aoTupleId;
						
						/* inserting into an append only relation */
						appendonly_insert(resultRelInfo->ri_aoInsertDesc, tuple, &tupleOid, &aoTupleId);
						
						if (resultRelInfo->ri_NumIndices > 0)
							ExecInsertIndexTuples(slot, (ItemPointer)&aoTupleId, estate, false);
					}
					else if (relstorage == RELSTORAGE_PARQUET)
					{
						AOTupleId aoTupleId;

						parquet_insert_values(resultRelInfo->ri_parquetInsertDesc, values, nulls, &aoTupleId);

						if (resultRelInfo->ri_NumIndices > 0)
							ExecInsertIndexTuples(slot, (ItemPointer)&aoTupleId, estate, false);
					}
					else if (relstorage == RELSTORAGE_EXTERNAL)
					{
						external_insert(resultRelInfo->ri_extInsertDesc, tuple);
					}
					else
					{
						simple_heap_insert(resultRelInfo->ri_RelationDesc, tuple);

						if (resultRelInfo->ri_NumIndices > 0)
							ExecInsertIndexTuples(slot, &(((HeapTuple)tuple)->t_self), estate, false);
					}


					/* AFTER ROW INSERT Triggers */
					ExecARInsertTriggers(estate, resultRelInfo, tuple);

					/*
					 * We count only tuples not suppressed by a BEFORE INSERT trigger;
					 * this is the same definition used by execMain.c for counting
					 * tuples inserted by an INSERT command.
					 *
					 * MPP: incrementing this counter here only matters for utility
					 * mode. in dispatch mode only the dispatcher COPY collects row
					 * count, so this counter is meaningless.
					 */
					cstate->processed++;
					if (relstorage_is_ao(relstorage))
						resultRelInfo->ri_aoprocessed++;
				}

				RESET_LINEBUF;
			}					/* end while(!raw_buf_done) */
		}						/* end if (bytesread > 0 || !cstate->fe_eof) */
		else
			/* no bytes read, end of data */
		{
			no_more_data = true;
		}
	} while (!no_more_data);


	/*
	 * Done, clean up
	 */
	error_context_stack = errcontext.previous;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Execute AFTER STATEMENT insertion triggers
	 */
	ExecASInsertTriggers(estate, resultRelInfo);

	/*
	 * Handle queued AFTER triggers
	 */
	AfterTriggerEndQuery(estate);

	/*
	 * If SREH and in executor mode send the number of rejected
	 * rows to the client (QD COPY).
	 */
	if(cstate->errMode != ALL_OR_NOTHING && Gp_role == GP_ROLE_EXECUTE)
		SendNumRowsRejected(cstate->cdbsreh->rejectcount);

	if (estate->es_result_partitions && Gp_role == GP_ROLE_EXECUTE)
		SendAOTupCounts(estate);

	/* free the hash table allocated by values_get_partition(), if any */
	if(estate->es_result_partitions && estate->es_partition_state->result_partition_hash != NULL)
		hash_destroy(estate->es_partition_state->result_partition_hash);
		
	pfree(attr_offsets);

	pfree(in_functions);
	pfree(typioparams);
	pfree(defmap);
	pfree(defexprs);

	ExecDropSingleTupleTableSlot(slot);

	StringInfo buf = NULL;
	int aocount = 0;

	resultRelInfo = estate->es_result_relations;
	for (i = 0; i < estate->es_num_result_relations; i++)
	{
		if (resultRelInfo->ri_aoInsertDesc)
			++aocount;
		if (resultRelInfo->ri_parquetInsertDesc)
			++aocount;
		resultRelInfo++;
	}

	if (Gp_role == GP_ROLE_EXECUTE && aocount > 0)
		buf = PreSendbackChangedCatalog(aocount);

	/*
	 * Finalize appends and close relations we opened.
	 */
	resultRelInfo = estate->es_result_relations;
	for (i = estate->es_num_result_relations; i > 0; i--)
	{
		QueryContextDispatchingSendBack sendback = NULL;

		if (resultRelInfo->ri_aoInsertDesc)
		{
			sendback = CreateQueryContextDispatchingSendBack(1);
			resultRelInfo->ri_aoInsertDesc->sendback = sendback;
			sendback->relid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

			appendonly_insert_finish(resultRelInfo->ri_aoInsertDesc);
		}

		if (resultRelInfo->ri_parquetInsertDesc)
		{
			sendback = CreateQueryContextDispatchingSendBack(
					resultRelInfo->ri_parquetInsertDesc->parquet_rel->rd_att->natts);
			resultRelInfo->ri_parquetInsertDesc->sendback = sendback;
			sendback->relid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

			parquet_insert_finish(resultRelInfo->ri_parquetInsertDesc);
		}

		if (resultRelInfo->ri_extInsertDesc)
				external_insert_finish(resultRelInfo->ri_extInsertDesc);
			
		if (sendback && relstorage_is_ao(RelinfoGetStorage(resultRelInfo)) && Gp_role == GP_ROLE_EXECUTE)
			AddSendbackChangedCatalogContent(buf, sendback);

		DropQueryContextDispatchingSendBack(sendback);

		/* Close indices and then the relation itself */
		ExecCloseIndices(resultRelInfo);
		heap_close(resultRelInfo->ri_RelationDesc, NoLock);
		resultRelInfo++;
	}
	
	if (Gp_role == GP_ROLE_EXECUTE && aocount > 0)
		FinishSendbackChangedCatalog(buf);

	cstate->rel = NULL; /* closed above */
	FreeExecutorState(estate);
}

/*
 * Finds the next TEXT line that is in the input buffer and loads
 * it into line_buf. Returns an indication if the line that was read
 * is complete (if an unescaped line-end was encountered). If we
 * reached the end of buffer before the whole line was written into the
 * line buffer then returns false.
 */
bool
CopyReadLineText(CopyState cstate, size_t bytesread)
{
	int			linesize;
	char		escapec = '\0';

	/* mark that encoding conversion hasn't occurred yet */
	cstate->line_buf_converted = false;

	/*
	 * set the escape char for text format ('\\' by default).
	 */
	escapec = cstate->escape[0];

	/*
	 * Detect end of line type if not already detected.
	 */
	if (cstate->eol_type == EOL_UNKNOWN)
	{
		cstate->quote = NULL;

		if (!DetectLineEnd(cstate, bytesread))
		{
			/* load entire input buffer into line buf, and quit */
			appendBinaryStringInfo(&cstate->line_buf, cstate->raw_buf, bytesread);
			cstate->raw_buf_done = true;
			cstate->line_done = CopyCheckIsLastLine(cstate);

			if (cstate->line_done)
				preProcessDataLine(cstate);

			return cstate->line_done;
		}
	}

	/*
	 * Special case: eol is CRNL, last byte of previous buffer was an
	 * unescaped CR and 1st byte of current buffer is NL. We check for
	 * that here.
	 */
	if (cstate->eol_type == EOL_CRLF)
	{
		/* if we started scanning from the 1st byte of the buffer */
		if (cstate->begloc == cstate->raw_buf)
		{
			/* and had a CR in last byte of prev buf */
			if (cstate->cr_in_prevbuf)
			{
				/*
				 * if this 1st byte in buffer is 2nd byte of line end sequence
				 * (linefeed)
				 */
				if (*(cstate->begloc) == cstate->eol_ch[1])
				{
					/*
					* load that one linefeed byte and indicate we are done
					* with the data line
					*/
					appendBinaryStringInfo(&cstate->line_buf, cstate->begloc, 1);
					cstate->raw_buf_index++;
					cstate->begloc++;
					cstate->cr_in_prevbuf = false;
					preProcessDataLine(cstate);

					return true;
				}
			}

			cstate->cr_in_prevbuf = false;
		}
	}

	/*
	 * (we need a loop so that if eol_ch is found, but prev ch is backslash,
	 * we can search for the next eol_ch)
	 */
	while (true)
	{
		/* reached end of buffer */
		if ((cstate->endloc = scanTextLine(cstate, cstate->begloc, cstate->eol_ch[0], bytesread - cstate->raw_buf_index)) == NULL)
		{
			linesize = bytesread - (cstate->begloc - cstate->raw_buf);
			appendBinaryStringInfo(&cstate->line_buf, cstate->begloc, linesize);

			if (cstate->eol_type == EOL_CRLF && cstate->line_buf.len > 1)
			{
				char	   *last_ch = cstate->line_buf.data + cstate->line_buf.len - 1; /* before terminating \0 */

				if (*last_ch == '\r')
					cstate->cr_in_prevbuf = true;
			}

			cstate->line_done = CopyCheckIsLastLine(cstate);
			cstate->raw_buf_done = true;

			break;
		}
		else
			/* found the 1st eol ch in raw_buf. */
		{
			bool		eol_found = true;

			/*
			 * Load that piece of data (potentially a data line) into the line buffer,
			 * and update the pointers for the next scan.
			 */
			linesize = cstate->endloc - cstate->begloc + 1;
			appendBinaryStringInfo(&cstate->line_buf, cstate->begloc, linesize);
			cstate->raw_buf_index += linesize;
			cstate->begloc = cstate->endloc + 1;

			if (cstate->eol_type == EOL_CRLF)
			{
				/* check if there is a '\n' after the '\r' */
				if (*(cstate->endloc + 1) == '\n')
				{
					/* this is a line end */
					appendBinaryStringInfo(&cstate->line_buf, cstate->begloc, 1);		/* load that '\n' */
					cstate->raw_buf_index++;
					cstate->begloc++;
				}
				else
					/* just a CR, not a line end */
					eol_found = false;
			}

			/*
			 * in some cases, this end of line char happens to be the
			 * last character in the buffer. we need to catch that.
			 */
			if (cstate->raw_buf_index >= bytesread)
				cstate->raw_buf_done = true;

			/*
			 * if eol was found, and it isn't escaped, line is done
			 */
			if (eol_found)
			{
				cstate->line_done = true;
				break;
			}
			else
			{
				/* stay in the loop and process some more data. */
				cstate->line_done = false;

				if (eol_found)
					cstate->cur_lineno++;		/* increase line index for error
												 * reporting */
			}

		}						/* end of found eol_ch */
	}

	/* Done reading a complete line. Do pre processing of the raw input data */
	if (cstate->line_done)
		preProcessDataLine(cstate);

	/*
	 * check if this line is an end marker -- "\."
	 */
	cstate->end_marker = false;

	switch (cstate->eol_type)
	{
		case EOL_LF:
			if (!strcmp(cstate->line_buf.data, "\\.\n"))
				cstate->end_marker = true;
			break;
		case EOL_CR:
			if (!strcmp(cstate->line_buf.data, "\\.\r"))
				cstate->end_marker = true;
			break;
		case EOL_CRLF:
			if (!strcmp(cstate->line_buf.data, "\\.\r\n"))
				cstate->end_marker = true;
			break;
		case EOL_UNKNOWN:
			break;
	}

	if (cstate->end_marker)
	{
		/*
		 * Reached end marker. In protocol version 3 we
		 * should ignore anything after \. up to protocol
		 * end of copy data.
		 */
		if (cstate->copy_dest == COPY_NEW_FE)
		{
			while (!cstate->fe_eof)
			{
				CopyGetData(cstate, cstate->raw_buf, RAW_BUF_SIZE);	/* eat data */
			}
		}

		cstate->fe_eof = true;
		/* we don't want to process a \. as data line, want to quit. */
		cstate->line_done = false;
		cstate->raw_buf_done = true;
	}

	return cstate->line_done;
}

/*
 * Finds the next CSV line that is in the input buffer and loads
 * it into line_buf. Returns an indication if the line that was read
 * is complete (if an unescaped line-end was encountered). If we
 * reached the end of buffer before the whole line was written into the
 * line buffer then returns false.
 */
bool
CopyReadLineCSV(CopyState cstate, size_t bytesread)
{
	int			linesize;
	char		quotec = '\0',
				escapec = '\0';
	bool		csv_is_invalid = false;

	/* mark that encoding conversion hasn't occurred yet */
	cstate->line_buf_converted = false;

	escapec = cstate->escape[0];
	quotec = cstate->quote[0];

	/* ignore special escape processing if it's the same as quotec */
	if (quotec == escapec)
		escapec = '\0';

	/*
	 * Detect end of line type if not already detected.
	 */
	if (cstate->eol_type == EOL_UNKNOWN)
	{
		if (!DetectLineEnd(cstate, bytesread))
		{
			/* EOL not found. load entire input buffer into line buf, and return */
			appendBinaryStringInfo(&cstate->line_buf, cstate->raw_buf, bytesread);
			cstate->line_done = CopyCheckIsLastLine(cstate);;
			cstate->raw_buf_done = true;

			if (cstate->line_done)
				preProcessDataLine(cstate);

			return cstate->line_done;
		}
	}

	/*
	 * Special case: eol is CRNL, last byte of previous buffer was an
	 * unescaped CR and 1st byte of current buffer is NL. We check for
	 * that here.
	 */
	if (cstate->eol_type == EOL_CRLF)
	{
		/* if we started scanning from the 1st byte of the buffer */
		if (cstate->begloc == cstate->raw_buf)
		{
			/* and had a CR in last byte of prev buf */
			if (cstate->cr_in_prevbuf)
			{
				/*
				 * if this 1st byte in buffer is 2nd byte of line end sequence
				 * (linefeed)
				 */
				if (*(cstate->begloc) == cstate->eol_ch[1])
				{
					/*
					 * load that one linefeed byte and indicate we are done
					 * with the data line
					 */
					appendBinaryStringInfo(&cstate->line_buf, cstate->begloc, 1);
					cstate->raw_buf_index++;
					cstate->begloc++;
					cstate->line_done = true;
					preProcessDataLine(cstate);
					cstate->cr_in_prevbuf = false;

					return true;
				}
			}

			cstate->cr_in_prevbuf = false;
		}
	}

	/*
	 * (we need a loop so that if eol_ch is found, but we are in quotes,
	 * we can search for the next eol_ch)
	 */
	while (true)
	{
		/* reached end of buffer */
		if ((cstate->endloc = scanCSVLine(cstate, cstate->begloc, cstate->eol_ch[0], escapec, quotec, bytesread - cstate->raw_buf_index)) == NULL)
		{
			linesize = bytesread - (cstate->begloc - cstate->raw_buf);
			appendBinaryStringInfo(&cstate->line_buf, cstate->begloc, linesize);

			if (cstate->line_buf.len > 1)
			{
				char	   *last_ch = cstate->line_buf.data + cstate->line_buf.len - 1; /* before terminating \0 */

				if (*last_ch == '\r')
				{
					if (cstate->eol_type == EOL_CRLF)
						cstate->cr_in_prevbuf = true;
				}
			}

			cstate->line_done = CopyCheckIsLastLine(cstate);
			cstate->raw_buf_done = true;
			break;
		}
		else
			/* found 1st eol char in raw_buf. */
		{
			bool		eol_found = true;

			/*
			 * Load that piece of data (potentially a data line) into the line buffer,
			 * and update the pointers for the next scan.
			 */
			linesize = cstate->endloc - cstate->begloc + 1;
			appendBinaryStringInfo(&cstate->line_buf, cstate->begloc, linesize);
			cstate->raw_buf_index += linesize;
			cstate->begloc = cstate->endloc + 1;

			/* end of line only if not in quotes */
			if (cstate->in_quote)
			{
				/* buf done, but still in quote */
				if (cstate->raw_buf_index >= bytesread)
					cstate->raw_buf_done = true;

				cstate->line_done = false;

				/* update file line for error message */

				/*
				 * TODO: for dos line end we need to do check before
				 * incrementing!
				 */
				cstate->cur_lineno++;

				/*
				 * If we are still in quotes and linebuf len is extremely large
				 * then this file has bad csv and we have to stop the rolling
				 * snowball from getting bigger.
				 */
				if(cstate->line_buf.len >= gp_max_csv_line_length)
				{
					csv_is_invalid = true;
					cstate->in_quote = false;
					cstate->line_done = true;
					cstate->num_consec_csv_err++;
					break;
				}

				if (cstate->raw_buf_done)
					break;
			}
			else
			{
				/* if dos eol, check for '\n' after the '\r' */
				if (cstate->eol_type == EOL_CRLF)
				{
					if (*(cstate->endloc + 1) == '\n')
					{
						/* this is a line end */
						appendBinaryStringInfo(&cstate->line_buf, cstate->begloc, 1);	/* load that '\n' */
						cstate->raw_buf_index++;
						cstate->begloc++;
					}
					else
						/* just a CR, not a line end */
						eol_found = false;
				}

				/*
				 * in some cases, this end of line char happens to be the
				 * last character in the buffer. we need to catch that.
				 */
				if (cstate->raw_buf_index >= bytesread)
					cstate->raw_buf_done = true;

				/*
				 * if eol was found line is done
				 */
				if (eol_found)
				{
					cstate->line_done = true;
					break;
				}
			}
		}						/* end of found eol_ch */
	}


	/* Done reading a complete line. Do pre processing of the raw input data */
	if (cstate->line_done)
		preProcessDataLine(cstate);

	/*
	 * We have a corrupted csv format case. It is already converted to server
	 * encoding, *which is necessary*. Ok, we can report an error now.
	 */
	if(csv_is_invalid)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("data line too long. likely due to invalid csv data")));
	else
		cstate->num_consec_csv_err = 0; /* reset consecutive count */

	/*
	 * check if this line is an end marker -- "\."
	 */
	cstate->end_marker = false;

	switch (cstate->eol_type)
	{
		case EOL_LF:
			if (!strcmp(cstate->line_buf.data, "\\.\n"))
				cstate->end_marker = true;
			break;
		case EOL_CR:
			if (!strcmp(cstate->line_buf.data, "\\.\r"))
				cstate->end_marker = true;
			break;
		case EOL_CRLF:
			if (!strcmp(cstate->line_buf.data, "\\.\r\n"))
				cstate->end_marker = true;
			break;
		case EOL_UNKNOWN:
			break;
	}

	if (cstate->end_marker)
	{
		/*
		 * Reached end marker. In protocol version 3 we
		 * should ignore anything after \. up to protocol
		 * end of copy data.
		 */
		if (cstate->copy_dest == COPY_NEW_FE)
		{
			while (!cstate->fe_eof)
			{
				CopyGetData(cstate, cstate->raw_buf, RAW_BUF_SIZE);	/* eat data */
			}
		}

		cstate->fe_eof = true;
		/* we don't want to process a \. as data line, want to quit. */
		cstate->line_done = false;
		cstate->raw_buf_done = true;
	}

	return cstate->line_done;
}

/*
 * Detected the eol type by looking at the first data row.
 * Possible eol types are NL, CR, or CRNL. If eol type was
 * detected, it is set and a boolean true is returned to
 * indicated detection was successful. If the first data row
 * is longer than the input buffer, we return false and will
 * try again in the next buffer.
 */
static bool
DetectLineEnd(CopyState cstate, size_t bytesread  __attribute__((unused)))
{
	int			index = 0;
	int			lineno = 0;
	char		c;
	char		quotec = '\0',
				escapec = '\0';
	bool		csv = false;
	
	/*
	 * CSV special case. See MPP-7819.
	 * 
	 * this functions may change the in_quote value while processing.
	 * this is ok as we need to keep state in case we don't find EOL
	 * in this buffer and need to be called again to continue searching.
	 * BUT if EOL *was* found we must reset to the state we had since 
	 * we are about to reprocess this buffer again in CopyReadLineCSV
	 * from the same starting point as we are in right now. 
	 */
	bool save_inquote = cstate->in_quote;
	bool save_lastwas = cstate->last_was_esc;

	/* if user specified NEWLINE we should never be here */
	Assert(!cstate->eol_str);

	if (cstate->quote)					/* CSV format */
	{
		csv = true;
		quotec = cstate->quote[0];
		escapec = cstate->escape[0];
		/* ignore special escape processing if it's the same as quotec */
		if (quotec == escapec)
			escapec = '\0';
	}

	while (index < RAW_BUF_SIZE)
	{
		c = cstate->raw_buf[index];

		if (csv)
		{
			if (cstate->in_quote && c == escapec)
				cstate->last_was_esc = !cstate->last_was_esc;
			if (c == quotec && !cstate->last_was_esc)
				cstate->in_quote = !cstate->in_quote;
			if (c != escapec)
				cstate->last_was_esc = false;
		}

		if (c == '\n')
		{
			lineno++;
			
			if (!csv || (csv && !cstate->in_quote))
			{
				cstate->eol_type = EOL_LF;
				cstate->eol_ch[0] = '\n';
				cstate->eol_ch[1] = '\0';

				cstate->in_quote = save_inquote; /* see comment at declaration */
				cstate->last_was_esc = save_lastwas;
				return true;
			}
			else if(csv && cstate->in_quote && cstate->line_buf.len + index >= gp_max_csv_line_length)
			{	
				/* we do a "line too long" CSV check for the first row as well (MPP-7869) */
				cstate->in_quote = false;
				cstate->line_done = true;
				cstate->num_consec_csv_err++;
				cstate->cur_lineno += lineno;
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								errmsg("data line too long. likely due to invalid csv data")));
			}

		}
		if (c == '\r')
		{
			lineno++;
			
			if (!csv || (csv && !cstate->in_quote))
			{
				if (cstate->raw_buf[index + 1] == '\n')		/* always safe */
				{
					cstate->eol_type = EOL_CRLF;
					cstate->eol_ch[0] = '\r';
					cstate->eol_ch[1] = '\n';
				}
				else
				{
					cstate->eol_type = EOL_CR;
					cstate->eol_ch[0] = '\r';
					cstate->eol_ch[1] = '\0';
				}

				cstate->in_quote = save_inquote; /* see comment at declaration */
				cstate->last_was_esc = save_lastwas;
				return true;
			}
		}

		index++;
	}

	/* since we're yet to find the EOL this buffer will never be 
	 * re-processed so add the number of rows we found so we don't lose it */
	cstate->cur_lineno += lineno;
	
	return false;
}

/*
 *  Return decimal value for a hexadecimal digit
 */
static
int GetDecimalFromHex(char hex)
{
	if (isdigit((unsigned char)hex))
		return hex - '0';
	else
		return tolower((unsigned char)hex) - 'a' + 10;
}

/*
 * Read all TEXT attributes. Attributes are parsed from line_buf and
 * inserted (all at once) to attribute_buf, while saving pointers to
 * each attribute's starting position.
 *
 * When this routine finishes execution both the nulls array and
 * the attr_offsets array are updated. The attr_offsets will include
 * the offset from the beginning of the attribute array of which
 * each attribute begins. If a specific attribute is not used for this
 * COPY command (ommitted from the column list), a value of 0 will be assigned.
 * For example: for table foo(a,b,c,d,e) and COPY foo(a,b,e)
 * attr_offsets may look something like this after this routine
 * returns: [0,20,0,0,55]. That means that column "a" value starts
 * at byte offset 0, "b" in 20 and "e" in 55, in attribute_buf.
 *
 * In the attribute buffer (attribute_buf) each attribute
 * is terminated with a '\0', and therefore by using the attr_offsets
 * array we could point to a beginning of an attribute and have it
 * behave as a C string, much like previously done in COPY.
 *
 * Another aspect to improving performance is reducing the frequency
 * of data load into buffers. The original COPY read attribute code
 * loaded a character at a time. In here we try to load a chunk of data
 * at a time. Usually a chunk will include a full data row
 * (unless we have an escaped delim). That effectively reduces the number of
 * loads by a factor of number of bytes per row. This improves performance
 * greatly, unfortunately it add more complexity to the code.
 *
 * Global participants in parsing logic:
 *
 * line_buf.cursor -- an offset from beginning of the line buffer
 * that indicates where we are about to begin the next scan. Note that
 * if we have WITH OIDS or if we ran CopyExtractRowMetaData this cursor is
 * already shifted and is not in the beginning of line buf anymore.
 *
 * attribute_buf.cursor -- an offset from the beginning of the
 * attribute buffer that indicates where the current attribute begins.
 */

static inline uint64 uint64_has_nullbyte(uint64 w) {
return ((w - 0x0101010101010101ull) & ~w & 0x8080808080808080ull); }
static inline uint64 uint64_has_byte(uint64 w, unsigned char b) {
	w ^= b * 0x0101010101010101ull;
	return ((w - 0x0101010101010101ull) & ~w & 0x8080808080808080ull);}

void
CopyReadAttributesText(CopyState cstate, bool * __restrict nulls,
					   int * __restrict attr_offsets, int num_phys_attrs, Form_pg_attribute * __restrict attr)
{
	char		delimc = cstate->delim[0];		/* delimiter character */
	char		escapec = cstate->escape[0];	/* escape character    */
	char	   *scan_start;		/* pointer to line buffer for scan start. */
	char	   *scan_end;		/* pointer to line buffer where char was found */
	char	   *stop;
	char	   *scanner;
	int			attr_pre_len = 0;/* attr raw len, before processing escapes */
	int			attr_post_len = 0;/* current attr len after escaping */
	int			m;				/* attribute index being parsed */
	int			bytes_remaining;/* num bytes remaining to be scanned in line
								 * buf */
	int			chunk_start;	/* offset to beginning of line chunk to load */
	int			chunk_len = 0;	/* length of chunk of data to load to attr buf */
	int			oct_val;		/* byte value for octal escapes */
	int			hex_val;
	int			attnum = 0;		/* attribute number being parsed */
	int			attribute = 1;
	bool		saw_high_bit = false;
	ListCell   *cur;			/* cursor to attribute list used for this COPY */

	/* init variables for attribute scan */
	RESET_ATTRBUF;

	/* cursor is now > 0 if we copy WITH OIDS */
	scan_start = cstate->line_buf.data + cstate->line_buf.cursor;
	chunk_start = cstate->line_buf.cursor;

	cur = list_head(cstate->attnumlist);

	/* check for zero column table case */
	if(num_phys_attrs > 0)
	{
		attnum = lfirst_int(cur);
		m = attnum - 1;
	}

	if (cstate->escape_off)
		escapec = delimc;		/* look only for delimiters, escapes are
								 * disabled */

	/* have a single column only and no delim specified? take the fast track */
	if (cstate->delimiter_off)
    {
		CopyReadAttributesTextNoDelim(cstate, nulls, num_phys_attrs,
											 attnum);
        return;
    }

	/*
	 * Scan through the line buffer to read all attributes data
	 */
	while (cstate->line_buf.cursor < cstate->line_buf.len)
	{
		bytes_remaining = cstate->line_buf.len - cstate->line_buf.cursor;
		stop = scan_start + bytes_remaining;
		/*
		 * We can eliminate one test (for length) in the loop by replacing the
		 * last byte with the delimiter.  We need to remember what it was so we
		 * can replace it later.
		 */
		char  endchar = *(stop-1);
		*(stop-1) = delimc;

		/* Find the next of: delimiter, or escape, or end of buffer */
		for (scanner = scan_start; *scanner != delimc && *scanner != escapec; scanner++)
			;
		if (scanner == (stop-1) && endchar != delimc)
		{
			if (endchar != escapec)
				scanner++;
		}
		*(stop-1) = endchar;

		scan_end = (*scanner != '\0' ? (char *) scanner : NULL);

		if (scan_end == NULL)
		{
			/* GOT TO END OF LINE BUFFER */

			if (cur == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("extra data after last expected column"),
						 errOmitLocation(true)));

			attnum = lfirst_int(cur);
			m = attnum - 1;

			/* don't count eol char(s) in attr and chunk len calculation */
			if (cstate->eol_type == EOL_CRLF)
			{
				attr_pre_len += bytes_remaining - 2;
				chunk_len = cstate->line_buf.len - chunk_start - 2;
			}
			else
			{
				attr_pre_len += bytes_remaining - 1;
				chunk_len = cstate->line_buf.len - chunk_start - 1;
			}

			/* check if this is a NULL value or data value (assumed NULL) */
			if (attr_pre_len == cstate->null_print_len
				&&
				strncmp(cstate->line_buf.data + cstate->line_buf.len - attr_pre_len - 1, cstate->null_print, attr_pre_len)
				== 0)
				nulls[m] = true;
			else
				nulls[m] = false;

			attr_offsets[m] = cstate->attribute_buf.cursor;


			/* load the last chunk, the whole buffer in most cases */
			appendBinaryStringInfo(&cstate->attribute_buf, cstate->line_buf.data + chunk_start, chunk_len);

			cstate->line_buf.cursor += attr_pre_len + 2;		/* skip eol char and
														 * '\0' to exit loop */

			/*
			 * line is done, but do we have more attributes to process?
			 *
			 * normally, remaining attributes that have no data means ERROR,
			 * however, with FILL MISSING FIELDS remaining attributes become
			 * NULL. since attrs are null by default we leave unchanged and
			 * avoid throwing an error, with the exception of empty data lines
			 * for multiple attributes, which we intentionally don't support.
			 */
			if (lnext(cur) != NULL)
			{
				if (!cstate->fill_missing)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("missing data for column \"%s\"",
									 NameStr(attr[lfirst_int(lnext(cur)) - 1]->attname)),
							 errOmitLocation(true)));

				else if (attribute == 1 && attr_pre_len == 0)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("missing data for column \"%s\", found empty data line",
									 NameStr(attr[lfirst_int(lnext(cur)) - 1]->attname)),
							 errOmitLocation(true)));
			}
		}
		else
			/* FOUND A DELIMITER OR ESCAPE */
		{
			if (cur == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("extra data after last expected column"),
						 errOmitLocation(true)));

			if (*scan_end == delimc)	/* found a delimiter */
			{
				attnum = lfirst_int(cur);
				m = attnum - 1;

				/* (we don't include the delimiter ch in length) */
				attr_pre_len += scan_end - scan_start;
				attr_post_len += scan_end - scan_start;

				/* check if this is a null print or data (assumed NULL) */
				if (attr_pre_len == cstate->null_print_len &&
					strncmp(scan_end - attr_pre_len, cstate->null_print, attr_pre_len) == 0)
					nulls[m] = true;
				else
					nulls[m] = false;

				/* set the pointer to next attribute position */
				attr_offsets[m] = cstate->attribute_buf.cursor;

				/*
				 * update buffer cursors to our current location, +1 to skip
				 * the delimc
				 */
				cstate->line_buf.cursor = scan_end - cstate->line_buf.data + 1;
				cstate->attribute_buf.cursor += attr_post_len + 1;

				/* prepare scan for next attr */
				scan_start = cstate->line_buf.data + cstate->line_buf.cursor;
				cur = lnext(cur);
				attr_pre_len = 0;
				attr_post_len = 0;

				/*
				 * for the dispatcher - stop parsing once we have
				 * all the hash field values. We don't need the rest.
				 */
				if (Gp_role == GP_ROLE_DISPATCH)
				{
					if (attribute == cstate->last_hash_field)
					{
						/*
						 * load the chunk from chunk_start to end of current
						 * attribute, not including delimiter
						 */
						chunk_len = cstate->line_buf.cursor - chunk_start - 1;
						appendBinaryStringInfo(&cstate->attribute_buf, cstate->line_buf.data + chunk_start, chunk_len);
						break;
					}
				}

				attribute++;
			}
			else
				/* found an escape character */
			{
				char		nextc = *(scan_end + 1);
				char		newc;
				int			skip = 2;

				chunk_len = (scan_end - cstate->line_buf.data) - chunk_start + 1;

				/* load a chunk of data */
				appendBinaryStringInfo(&cstate->attribute_buf, cstate->line_buf.data + chunk_start, chunk_len);

				switch (nextc)
				{
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
						/* handle \013 */
						oct_val = OCTVALUE(nextc);
						nextc = *(scan_end + 2);

						/*
						 * (no need for out bad access check since line if
						 * buffered)
						 */
						if (ISOCTAL(nextc))
						{
							skip++;
							oct_val = (oct_val << 3) + OCTVALUE(nextc);
							nextc = *(scan_end + 3);
							if (ISOCTAL(nextc))
							{
								skip++;
								oct_val = (oct_val << 3) + OCTVALUE(nextc);
							}
						}
						newc = oct_val & 0377;	/* the escaped byte value */
						if (IS_HIGHBIT_SET(newc))
							saw_high_bit = true;
						break;
					case 'x':
						/* Handle \x3F */
						hex_val = 0; /* init */
						nextc = *(scan_end + 2); /* get char after 'x' */

						if (isxdigit((unsigned char)nextc))
						{
							skip++;
							hex_val = GetDecimalFromHex(nextc);
							nextc = *(scan_end + 3); /* get second char */

							if (isxdigit((unsigned char)nextc))
							{
								skip++;
								hex_val = (hex_val << 4) + GetDecimalFromHex(nextc);
							}
							newc = hex_val & 0xff;
							if (IS_HIGHBIT_SET(newc))
								saw_high_bit = true;
						}
						else
						{
							newc = 'x';
						}
						break;

					case 'b':
						newc = '\b';
						break;
					case 'f':
						newc = '\f';
						break;
					case 'n':
						newc = '\n';
						break;
					case 'r':
						newc = '\r';
						break;
					case 't':
						newc = '\t';
						break;
					case 'v':
						newc = '\v';
						break;
					default:
						if (nextc == delimc)
							newc = delimc;
						else if (nextc == escapec)
							newc = escapec;
						else
						{
							/* no escape sequence found. it's a lone escape */
							
							bool next_is_eol = ((nextc == '\n' && cstate->eol_type == EOL_LF) ||
											    (nextc == '\r' && (cstate->eol_type == EOL_CR || 
																   cstate->eol_type == EOL_CRLF)));
							
							if(!next_is_eol)
							{
								/* take next char literally */
								newc = nextc;
							}
							else
							{
								/* there isn't a next char (end of data in line). we keep the 
								 * backslash as a literal character. We don't skip over the EOL,
								 * since we don't support escaping it anymore (unlike PG).
								 */
								newc = escapec;
								skip--;
							}
						}

						break;
				}

				/* update to current length, add escape and escaped chars  */
				attr_pre_len += scan_end - scan_start + 2;
				/* update to current length, escaped char */
				attr_post_len += scan_end - scan_start + 1;

				/*
				 * Need to get rid of the escape character. This is done by
				 * loading the chunk up to including the escape character
				 * into the attribute buffer. Then overwriting the escape char
				 * with the escaped sequence or char, and continuing to scan
				 * from *after* the char than is after the escape in line_buf.
				 */
				*(cstate->attribute_buf.data + cstate->attribute_buf.len - 1) = newc;
				cstate->line_buf.cursor = scan_end - cstate->line_buf.data + skip;
				scan_start = scan_end + skip;
				chunk_start = cstate->line_buf.cursor;
				chunk_len = 0;
			}

		}						/* end delimiter/backslash */

	}							/* end line buffer scan. */

	/*
	 * Replace all delimiters with NULL for string termination.
	 * NOTE: only delimiters (NOT necessarily all delimc) are replaced.
	 * Example (delimc = '|'):
	 * - Before:  f  1	|  f  \|  2  |	f  3
	 * - After :  f  1 \0  f   |  2 \0	f  3
	 */
	for (attribute = 0; attribute < num_phys_attrs; attribute++)
	{
		if (attr_offsets[attribute] != 0)
			*(cstate->attribute_buf.data + attr_offsets[attribute] - 1) = '\0';
	}

	/* 
	 * MPP-6816 
	 * If any attribute has a de-escaped octal or hex sequence with a
	 * high bit set, we check that the changed attribute text is still
	 * valid WRT encoding. We run the check on all attributes since 
	 * such octal sequences are so rare in client data that it wouldn't
	 * affect performance at all anyway.
	 */
	if(saw_high_bit)
	{
		for (attribute = 0; attribute < num_phys_attrs; attribute++)
		{
			char *fld = cstate->attribute_buf.data + attr_offsets[attribute];
			pg_verifymbstr(fld, strlen(fld), false);
		}
	}
}

/*
 * Read all the attributes of the data line in CSV mode,
 * performing de-escaping as needed. Escaping does not follow the normal
 * PostgreSQL text mode, but instead "standard" (i.e. common) CSV usage.
 *
 * Quoted fields can span lines, in which case the line end is embedded
 * in the returned string.
 *
 * null_print is the null marker string.  Note that this is compared to
 * the pre-de-escaped input string (thus if it is quoted it is not a NULL).
 *----------
 */
void
CopyReadAttributesCSV(CopyState cstate, bool *nulls, int *attr_offsets,
					  int num_phys_attrs, Form_pg_attribute *attr)
{
	char		delimc = cstate->delim[0];
	char		quotec = cstate->quote[0];
	char		escapec = cstate->escape[0];
	char		c;
	int			start_cursor = cstate->line_buf.cursor;
	int			end_cursor = start_cursor;
	int			input_len = 0;
	int			attnum;			/* attribute number being parsed */
	int			m = 0;			/* attribute index being parsed */
	int			attribute = 1;
	bool		in_quote = false;
	bool		saw_quote = false;
	ListCell   *cur;			/* cursor to attribute list used for this COPY */

	/* init variables for attribute scan */
	RESET_ATTRBUF;

	cur = list_head(cstate->attnumlist);

	if(num_phys_attrs > 0)
	{
		attnum = lfirst_int(cur);
		m = attnum - 1;
	}

	for (;;)
	{
		end_cursor = cstate->line_buf.cursor;

		/* finished processing attributes in line */
		if (cstate->line_buf.cursor >= cstate->line_buf.len - 1)
		{
			input_len = end_cursor - start_cursor;

			if (cstate->eol_type == EOL_CRLF)
			{
				/* ignore the leftover CR */
				input_len--;
				cstate->attribute_buf.data[cstate->attribute_buf.cursor - 1] = '\0';
			}

			/* check whether raw input matched null marker */
			if(num_phys_attrs > 0)
			{
				if (!saw_quote && input_len == cstate->null_print_len &&
					strncmp(&cstate->line_buf.data[start_cursor], cstate->null_print, input_len) == 0)
					nulls[m] = true;
				else
					nulls[m] = false;
			}

			/* if zero column table and data is trying to get in */
			if(num_phys_attrs == 0 && input_len > 0)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("extra data after last expected column"),
						 errOmitLocation(true)));

			if (in_quote)
			{
				/* next c will usually be LF, but it could also be a quote
				 * char if the last line of the file has no LF, and we don't
				 * want to error out in this case.
				 */
				c = cstate->line_buf.data[cstate->line_buf.cursor];
				if(c != quotec)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unterminated CSV quoted field"),
							 errOmitLocation(true)));
			}

			/*
			 * line is done, but do we have more attributes to process?
			 *
			 * normally, remaining attributes that have no data means ERROR,
			 * however, with FILL MISSING FIELDS remaining attributes become
			 * NULL. since attrs are null by default we leave unchanged and
			 * avoid throwing an error, with the exception of empty data lines
			 * for multiple attributes, which we intentionally don't support.
			 */
			if (lnext(cur) != NULL)
			{
				if (!cstate->fill_missing)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("missing data for column \"%s\"",
									NameStr(attr[m + 1]->attname)),
							 errOmitLocation(true)));

				else if (attribute == 1 && input_len == 0)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("missing data for column \"%s\", found empty data line",
									NameStr(attr[m + 1]->attname)),
							 errOmitLocation(true)));
			}

			break;
		}

		c = cstate->line_buf.data[cstate->line_buf.cursor++];

		/* unquoted field delimiter  */
		if (!in_quote && c == delimc && !cstate->delimiter_off)
		{
			/* check whether raw input matched null marker */
			input_len = end_cursor - start_cursor;

			if(num_phys_attrs > 0)
			{
				if (!saw_quote && input_len == cstate->null_print_len &&
				strncmp(&cstate->line_buf.data[start_cursor], cstate->null_print, input_len) == 0)
					nulls[m] = true;
				else
					nulls[m] = false;
			}

			/* terminate attr string with '\0' */
			appendStringInfoCharMacro(&cstate->attribute_buf, '\0');
			cstate->attribute_buf.cursor++;

			/* setup next attribute scan */
			cur = lnext(cur);

			if (cur == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("extra data after last expected column"),
						 errOmitLocation(true)));

			saw_quote = false;

			if(num_phys_attrs > 0)
			{
				attnum = lfirst_int(cur);
				m = attnum - 1;
				attr_offsets[m] = cstate->attribute_buf.cursor;
			}

			start_cursor = cstate->line_buf.cursor;

			/*
			 * for the dispatcher - stop parsing once we have
			 * all the hash field values. We don't need the rest.
			 */
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				if (attribute == cstate->last_hash_field)
					break;
			}

			attribute++;
			continue;
		}

		/* start of quoted field (or part of field) */
		if (!in_quote && c == quotec)
		{
			saw_quote = true;
			in_quote = true;
			continue;
		}

		/* escape within a quoted field */
		if (in_quote && c == escapec)
		{
			/*
			 * peek at the next char if available, and escape it if it is
			 * an escape char or a quote char
			 */
			if (cstate->line_buf.cursor <= cstate->line_buf.len)
			{
				char		nextc = cstate->line_buf.data[cstate->line_buf.cursor];

				if (nextc == escapec || nextc == quotec)
				{
					appendStringInfoCharMacro(&cstate->attribute_buf, nextc);
					cstate->line_buf.cursor++;
					cstate->attribute_buf.cursor++;
					continue;
				}
			}
		}

		/*
		 * end of quoted field. Must do this test after testing for escape
		 * in case quote char and escape char are the same (which is the
		 * common case).
		 */
		if (in_quote && c == quotec)
		{
			in_quote = false;
			continue;
		}
		appendStringInfoCharMacro(&cstate->attribute_buf, c);
		cstate->attribute_buf.cursor++;
	}

}

/*
 * Read a single attribute line when delimiter is 'off'. This is a fast track -
 * we copy the entire line buf into the attribute buf, check for null value,
 * and we're done.
 *
 * Note that no equivalent function exists for CSV, as in CSV we still may
 * need to parse quotes etc. so the functionality of delimiter_off is inlined
 * inside of CopyReadAttributesCSV
 */
static void
CopyReadAttributesTextNoDelim(CopyState cstate, bool *nulls, int num_phys_attrs,
							  int attnum)
{
	int 	len = 0;

	Assert(num_phys_attrs == 1);

	/* don't count eol char(s) in attr len calculation */
	len = cstate->line_buf.len - 1;

	if (cstate->eol_type == EOL_CRLF)
		len--;

	/* check if this is a NULL value or data value (assumed NULL) */
	if (len == cstate->null_print_len &&
		strncmp(cstate->line_buf.data, cstate->null_print, len) == 0)
		nulls[attnum - 1] = true;
	else
		nulls[attnum - 1] = false;

	appendBinaryStringInfo(&cstate->attribute_buf, cstate->line_buf.data, len);
}

/*
 * Read the first attribute. This is mainly used to maintain support
 * for an OID column. All the rest of the columns will be read at once with
 * CopyReadAttributesText.
 */
static char *
CopyReadOidAttr(CopyState cstate, bool *isnull)
{
	char		delimc = cstate->delim[0];
	char	   *start_loc = cstate->line_buf.data + cstate->line_buf.cursor;
	char	   *end_loc;
	int			attr_len = 0;
	int			bytes_remaining;

	/* reset attribute buf to empty */
	RESET_ATTRBUF;

	/* # of bytes that were not yet processed in this line */
	bytes_remaining = cstate->line_buf.len - cstate->line_buf.cursor;

	/* got to end of line */
	if ((end_loc = scanTextLine(cstate, start_loc, delimc, bytes_remaining)) == NULL)
	{
		attr_len = bytes_remaining - 1; /* don't count '\n' in len calculation */
		appendBinaryStringInfo(&cstate->attribute_buf, start_loc, attr_len);
		cstate->line_buf.cursor += attr_len + 2;		/* skip '\n' and '\0' */
	}
	else
		/* found a delimiter */
	{
		/*
		 * (we don't care if delim was preceded with a backslash, because it's
		 * an invalid OID anyway)
		 */

		attr_len = end_loc - start_loc; /* we don't include the delimiter ch */

		appendBinaryStringInfo(&cstate->attribute_buf, start_loc, attr_len);
		cstate->line_buf.cursor += attr_len + 1;
	}


	/* check whether raw input matched null marker */
	if (attr_len == cstate->null_print_len && strncmp(start_loc, cstate->null_print, attr_len) == 0)
		*isnull = true;
	else
		*isnull = false;

	return cstate->attribute_buf.data;
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			CopySendData(cstate, start, ptr - start); \
	} while (0)

/*
 * Send text representation of one attribute, with conversion and escaping
 */
static void
CopyAttributeOutText(CopyState cstate, char *string)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->delim[0];
	char		escapec = cstate->escape[0];

	if (cstate->need_transcoding)
		ptr = pg_server_to_custom(string, 
								  strlen(string), 
								  cstate->client_encoding, 
								  cstate->enc_conversion_proc);
	else
		ptr = string;


	if (cstate->escape_off)
	{
		CopySendData(cstate, ptr, strlen(ptr));
		return;
	}

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.	To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "ptr"
	 * can be sent literally, but hasn't yet been.
	 *
	 * We can skip pg_encoding_mblen() overhead when encoding is safe, because
	 * in valid backend encodings, extra bytes of a multibyte character never
	 * look like ASCII.  This loop is sufficiently performance-critical that
	 * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
	 * of the normal safe-encoding path.
	 */
	if (cstate->encoding_embeds_ascii)
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;		/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == escapec || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr++;	/* we include char in next run */
			}
			else if (IS_HIGHBIT_SET(c))
				ptr += pg_encoding_mblen(cstate->client_encoding, ptr);
			else
				ptr++;
		}
	}
	else
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;		/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == escapec || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr++;	/* we include char in next run */
			}
			else
				ptr++;
		}
	}

	DUMPSOFAR();
}

/*
 * Send text representation of one attribute, with conversion and
 * CSV-style escaping
 */
static void
CopyAttributeOutCSV(CopyState cstate, char *string,
					bool use_quote, bool single_attr)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->delim[0];
	char		quotec;
	char		escapec = cstate->escape[0];

	/*
	 * MPP-8075. We may get called with cstate->quote == NULL.
	 */
	if (cstate->quote == NULL)
	{
		quotec = '"';
	}
	else
	{
		quotec = cstate->quote[0];
	}

	/* force quoting if it matches null_print (before conversion!) */
	if (!use_quote && strcmp(string, cstate->null_print) == 0)
		use_quote = true;

	if (cstate->need_transcoding)
		ptr = pg_server_to_custom(string, 
								  strlen(string),
								  cstate->client_encoding,
								  cstate->enc_conversion_proc);
	else
		ptr = string;

	/*
	 * Make a preliminary pass to discover if it needs quoting
	 */
	if (!use_quote)
	{
		/*
		 * Because '\.' can be a data value, quote it if it appears alone on a
		 * line so it is not interpreted as the end-of-data marker.
		 */
		if (single_attr && strcmp(ptr, "\\.") == 0)
			use_quote = true;
		else
		{
			char	   *tptr = ptr;

			while ((c = *tptr) != '\0')
			{
				if (c == delimc || c == quotec || c == '\n' || c == '\r')
				{
					use_quote = true;
					break;
				}
				if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
					tptr += pg_encoding_mblen(cstate->client_encoding, tptr);
				else
					tptr++;
			}
		}
	}

	if (use_quote)
	{
		CopySendChar(cstate, quotec);

		/*
		 * We adopt the same optimization strategy as in CopyAttributeOutText
		 */
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if (c == quotec || c == escapec)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr;	/* we include char in next run */
			}
			if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
				ptr += pg_encoding_mblen(cstate->client_encoding, ptr);
			else
				ptr++;
		}
		DUMPSOFAR();

		CopySendChar(cstate, quotec);
	}
	else
	{
		/* If it doesn't need quoting, we can just dump it as-is */
		CopySendString(cstate, ptr);
	}
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * rel can be NULL ... it's only used for error reports.
 */
List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		Form_pg_attribute *attr = tupDesc->attrs;
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (attr[i]->attisdropped)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				if (tupDesc->attrs[i]->attisdropped)
					continue;
				if (namestrcmp(&(tupDesc->attrs[i]->attname), name) == 0)
				{
					attnum = tupDesc->attrs[i]->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									name, RelationGetRelationName(rel)),
							 errOmitLocation(true)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name),
							 errOmitLocation(true)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}

#define COPY_FIND_MD_DELIM \
md_delim = memchr(line_start, COPY_METADATA_DELIM, Min(32, cstate->line_buf.len)); \
if(md_delim && (md_delim != line_start)) \
{ \
	value_len = md_delim - line_start + 1; \
	*md_delim = '\0'; \
} \
else \
{ \
	cstate->md_error = true; \
}	

/*
 * CopyExtractRowMetaData - extract embedded row number from data.
 *
 * If data is being parsed in execute mode the parser (QE) doesn't
 * know the original line number (in the original file) of the current
 * row. Therefore the QD sends this information along with the data.
 * other metadata that the QD sends includes whether the data was
 * converted to server encoding (should always be the case, unless
 * encoding error happened and we're in error table mode).
 *
 * in:
 *    line_buf: <original_num>^<buf_converted>^<data for this row>
 *    lineno: ?
 *    line_buf_converted: ?
 *
 * out:
 *    line_buf: <data for this row>
 *    lineno: <original_num>
 *    line_buf_converted: <t/f>
 */
static
void CopyExtractRowMetaData(CopyState cstate)
{
	char *md_delim = NULL; /* position of the metadata delimiter */
	
	/*
	 * Line_buf may have already skipped an OID column if WITH OIDS defined,
	 * so we need to start from cursor not always from beginning of linebuf.
	 */
	char *line_start = cstate->line_buf.data + cstate->line_buf.cursor;
	int  value_len = 0;

	cstate->md_error = false;
	
	/* look for the first delimiter, and extract lineno */
	COPY_FIND_MD_DELIM;
	
	/* 
	 * make sure MD exists. that should always be the case
	 * unless we run into an edge case - see MPP-8052. if that 
	 * happens md_error is now set. we raise an error. 
	 */
	if(cstate->md_error)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("COPY metadata not found. This probably means that there is a "
						"mixture of newline types in the data. Use the NEWLINE keyword "
						"in order to resolve this reliably.")));

	cstate->cur_lineno = atoi(line_start);

	*md_delim = COPY_METADATA_DELIM; /* restore the line_buf byte after setting it to \0 */

	/* reposition line buf cursor to see next metadata value (skip lineno) */
	cstate->line_buf.cursor += value_len;
	line_start = cstate->line_buf.data + cstate->line_buf.cursor;

	/* look for the second delimiter, and extract line_buf_converted */
	COPY_FIND_MD_DELIM;
	Assert(*line_start == '0' || *line_start == '1'); 
	cstate->line_buf_converted = atoi(line_start);
	
	*md_delim = COPY_METADATA_DELIM;
	cstate->line_buf.cursor += value_len;
}

/*
 * error context callback for COPY FROM
 */
static void
copy_in_error_callback(void *arg)
{
	void *buff_orig;
	CopyState	cstate = (CopyState) arg;

	/*
	 * Make sure to not modify the actual line_buf here (for example
	 * in limit_printout_length() ) as we may got here not because of
	 * a data error but because of an innocent elog(NOTICE) for example
	 * somewhere in the code. We make a copy of line_buf and use it for
	 * the error context purposes so that a non-data related elog call
	 * won't corrupt our data in line_buf.
	 */
	StringInfoData copy_of_line_buf;
	char buffer[20];

	initStringInfo(&copy_of_line_buf);
	appendStringInfoString(&copy_of_line_buf, cstate->line_buf.data);

	/* NB:  be sure not to add anymore appendStringInfo() calls between here
	 *      and the pfree() at the bottom.
	 */
	buff_orig = copy_of_line_buf.data;

	/*
	 * If our (copy of) linebuf has the embedded original row number and other
	 * row-specific metadata, remove it. It is not part of the actual data, and 
	 * should not be displayed.
	 * 
	 * we skip this step, however, if md_error was previously set by
	 * CopyExtractRowMetaData. That should rarely happen, though.
	 */
	if(cstate->err_loc_type == ROWNUM_EMBEDDED && !cstate->md_error)
	{

		/* the following is a compacted mod of CopyExtractRowMetaData */
		int value_len = 0;
		char *line_start = copy_of_line_buf.data;
		char *lineno_delim = memchr(line_start, COPY_METADATA_DELIM, Min(32, cstate->line_buf.len));

		if (lineno_delim && (lineno_delim != line_start))
		{
			/*
			 * we only continue parsing metadata if the first extraction above
			 * succeeded. there are some edge cases where we may not have a line
			 * with MD to parse, for example if some non-copy related error 
			 * propagated here and we don't yet have a proper data line. 
			 * see MPP-11328
			 */
			value_len = lineno_delim - line_start + 1;
			copy_of_line_buf.data += value_len; /* advance beyond original lineno */
			copy_of_line_buf.len -= value_len;

			line_start = copy_of_line_buf.data;
			lineno_delim = memchr(line_start, COPY_METADATA_DELIM, Min(32, cstate->line_buf.len));

			value_len = lineno_delim - line_start + 1;
			copy_of_line_buf.data += value_len; /* advance beyond line_buf_converted */
			copy_of_line_buf.len -= value_len;		
		}
	}

	/*
	 * If we saved the error context from a QE in cdbcopy.c append it here.
	 */
	if (Gp_role == GP_ROLE_DISPATCH && cstate->executor_err_context.len > 0)
	{
		errcontext("%s", cstate->executor_err_context.data);
		pfree(buff_orig);
		return;
	}

	if (cstate->cur_attname)
	{
		/* error is relevant to a particular column */
		limit_printout_length(&cstate->attribute_buf);

		if(!cstate->error_on_executor) /* don't print out context if error wasn't local */
			errcontext("COPY %s, line %s, column %s",
					   cstate->cur_relname,linenumber_atoi(buffer,cstate->cur_lineno), cstate->cur_attname);
	}
	else
	{

		/* error is relevant to a particular line */
		if (cstate->line_buf_converted || !cstate->need_transcoding)
		{
			truncateEol(&copy_of_line_buf, cstate->eol_type);
			limit_printout_length(&copy_of_line_buf);

			if(!cstate->error_on_executor) /* don't print out context if error wasn't local */
				errcontext("COPY %s, line %s: \"%s\"", cstate->cur_relname,linenumber_atoi(buffer,cstate->cur_lineno), copy_of_line_buf.data);
		}
		else
		{
			/*
			 * Here, the line buffer is still in a foreign encoding,
			 * and indeed it's quite likely that the error is precisely
			 * a failure to do encoding conversion (ie, bad data).	We
			 * dare not try to convert it, and at present there's no way
			 * to regurgitate it without conversion.  So we have to punt
			 * and just report the line number.
			 */
			if(!cstate->error_on_executor)
				errcontext("COPY %s, line %s", cstate->cur_relname,linenumber_atoi(buffer,cstate->cur_lineno));
		}
	}
	pfree(buff_orig);
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * It would seem a lot easier to just use the sprintf "precision" limit to
 * truncate the string.  However, some versions of glibc have a bug/misfeature
 * that vsnprintf will always fail (return -1) if it is asked to truncate
 * a string that contains invalid byte sequences for the current encoding.
 * So, do our own truncation.  We assume we can alter the StringInfo buffer
 * holding the input data.
 */
void
limit_printout_length(StringInfo buf)
{
#define MAX_COPY_DATA_DISPLAY 100

	int			len;

	/* Fast path if definitely okay */
	if (buf->len <= MAX_COPY_DATA_DISPLAY)
		return;

	/* Apply encoding-dependent truncation */
	len = pg_mbcliplen(buf->data, buf->len, MAX_COPY_DATA_DISPLAY);
	if (buf->len <= len)
		return;					/* no need to truncate */
	buf->len = len;
	buf->data[len] = '\0';

	/* Add "..." to show we truncated the input */
	appendStringInfoString(buf, "...");
}


static void
attr_get_key(CopyState cstate, CdbCopy *cdbCopy, int original_lineno_for_qe,
			 unsigned int target_seg,
			 AttrNumber p_nattrs, AttrNumber *attrs,
			 Form_pg_attribute *attr_descs, int *attr_offsets, bool *attr_nulls,
			 FmgrInfo *in_functions, Oid *typioparams, Datum *values)
{
	AttrNumber p_index;

	/*
	 * Since we only need the internal format of values that
	 * we want to hash on (partitioning keys only), we want to
	 * skip converting the other values so we can run faster.
	 */
	for (p_index = 0; p_index < p_nattrs; p_index++)
	{
		ListCell *cur;

		/*
		 * For this partitioning key, search for its location in the attr list.
		 * (note that fields may be out of order, so this is necessary).
		 */
		foreach(cur, cstate->attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			char	   *string;
			bool		isnull;

			if (attnum == attrs[p_index])
			{
				string = cstate->attribute_buf.data + attr_offsets[m];

				if (attr_nulls[m])
					isnull = true;
				else
					isnull = false;

				if (cstate->csv_mode && isnull &&
					cstate->force_notnull_flags[m])
				{
					string = cstate->null_print;		/* set to NULL string */
					isnull = false;
				}

				/* we read an SQL NULL, no need to do anything */
				if (!isnull)
				{
					cstate->cur_attname = NameStr(attr_descs[m]->attname);

					values[m] = InputFunctionCall(&in_functions[m],
												  string,
												  typioparams[m],
												  attr_descs[m]->atttypmod);

					attr_nulls[m] = false;
					cstate->cur_attname = NULL;
				}		/* end if (!isnull) */

				break;	/* go to next partitioning key
						 * attribute */
			}
		}		/* end foreach */
	}			/* end for partitioning indexes */
}

/*
 * The following are custom versions of the string function strchr().
 * As opposed to the original strchr which searches through
 * a string until the target character is found, or a NULL is
 * found, this version will not return when a NULL is found.
 * Instead it will search through a pre-defined length of
 * bytes and will return only if the target character(s) is reached.
 *
 * If our client encoding is not a supported server encoding, we
 * know that it is not safe to look at each character as trailing
 * byte in a multibyte character may be a 7-bit ASCII equivalent.
 * Therefore we use pg_encoding_mblen to skip to the end of the
 * character.
 *
 * returns:
 *	 pointer to c - if c is located within the string.
 *	 NULL - if c was not found in specified length of search. Note:
 *			this DOESN'T mean that a '\0' was reached.
 */
char *
scanTextLine(CopyState cstate, const char *s, char eol, size_t len)
{
		
	if (cstate->encoding_embeds_ascii && !cstate->line_buf_converted)
	{
		int			mblen;
		const char *end = s + len;
		
		/* we may need to skip the end of a multibyte char from the previous buffer */
		s += cstate->missing_bytes;
		
		mblen = pg_encoding_mblen(cstate->client_encoding, s);

		for (; *s != eol && s < end; s += mblen)
			mblen = pg_encoding_mblen(cstate->client_encoding, s);

		/* 
		 * MPP-10802
		 * if last char is a partial mb char (the rest of its bytes are in the next 
		 * buffer) save # of missing bytes for this char and skip them next time around 
		 */
		cstate->missing_bytes = (s > end ? s - end : 0);
			
		return ((*s == eol) ? (char *) s : NULL);
	}
	else
		return memchr(s, eol, len);
}


char *
scanCSVLine(CopyState cstate, const char *s, char eol, char escapec, char quotec, size_t len)
{
	const char *start = s;
	const char *end = start + len;
	
	if (cstate->encoding_embeds_ascii && !cstate->line_buf_converted)
	{
		int			mblen;
		
		/* we may need to skip the end of a multibyte char from the previous buffer */
		s += cstate->missing_bytes;
		
		mblen = pg_encoding_mblen(cstate->client_encoding, s);
		
		for ( ; *s != eol && s < end ; s += mblen)
		{
			if (cstate->in_quote && *s == escapec)
				cstate->last_was_esc = !cstate->last_was_esc;
			if (*s == quotec && !cstate->last_was_esc)
				cstate->in_quote = !cstate->in_quote;
			if (*s != escapec)
				cstate->last_was_esc = false;

			mblen = pg_encoding_mblen(cstate->client_encoding, s);
		}
		
		/* 
		 * MPP-10802
		 * if last char is a partial mb char (the rest of its bytes are in the next 
		 * buffer) save # of missing bytes for this char and skip them next time around 
		 */
		cstate->missing_bytes = (s > end ? s - end : 0);
	}
	else
		/* safe to scroll byte by byte */
	{	
		for ( ; *s != eol && s < end ; s++)
		{
			if (cstate->in_quote && *s == escapec)
				cstate->last_was_esc = !cstate->last_was_esc;
			if (*s == quotec && !cstate->last_was_esc)
				cstate->in_quote = !cstate->in_quote;
			if (*s != escapec)
				cstate->last_was_esc = false;
		}
	}

	if (s == end)
		return NULL;
	
	if (*s == eol)
		cstate->last_was_esc = false;

	return ((*s == eol) ? (char *) s : NULL);
}

/* remove end of line chars from end of a buffer */
void truncateEol(StringInfo buf, EolType eol_type)
{
	int one_back = buf->len - 1;
	int two_back = buf->len - 2;

	if(eol_type == EOL_CRLF)
	{
		if(buf->len < 2)
			return;

		if(buf->data[two_back] == '\r' &&
		   buf->data[one_back] == '\n')
		{
			buf->data[two_back] = '\0';
			buf->data[one_back] = '\0';
			buf->len -= 2;
		}
	}
	else
	{
		if(buf->len < 1)
			return;

		if(buf->data[one_back] == '\r' ||
		   buf->data[one_back] == '\n')
		{
			buf->data[one_back] = '\0';
			buf->len--;
		}
	}
}

/*
 * concatenateEol
 *
 * add end of line chars to end line buf.
 *
 */
static void concatenateEol(CopyState cstate)
{
	switch (cstate->eol_type)
	{
		case EOL_LF:
			appendStringInfo(&cstate->line_buf, "\n");
			break;
		case EOL_CR:
			appendStringInfo(&cstate->line_buf, "\r");
			break;
		case EOL_CRLF:
			appendStringInfo(&cstate->line_buf, "\r\n");
			break;
		case EOL_UNKNOWN:
			appendStringInfo(&cstate->line_buf, "\n");
			break;

	}
}

/*
 * Escape any single quotes or backslashes in given string (from initdb.c)
 */
static char *
escape_quotes(const char *src)
{
	int			len = strlen(src),
				i,
				j;
	char	   *result = palloc(len * 2 + 1);

	for (i = 0, j = 0; i < len; i++)
	{
		if ((src[i]) == '\'' || (src[i]) == '\\')
			result[j++] = src[i];
		result[j++] = src[i];
	}
	result[j] = '\0';
	return result;
}

/*
 * copy_dest_startup --- executor startup
 */
static void
copy_dest_startup(DestReceiver *self __attribute__((unused)), int operation __attribute__((unused)), TupleDesc typeinfo __attribute__((unused)))
{
	/* no-op */
}

/*
 * copy_dest_receive --- receive one tuple
 */
static void
copy_dest_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_copy    *myState = (DR_copy *) self;
	CopyState	cstate = myState->cstate;

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	/* And send the data */
	CopyOneRowTo(cstate, InvalidOid, slot_get_values(slot), slot_get_isnull(slot));
}

/*
 * copy_dest_shutdown --- executor end
 */
static void
copy_dest_shutdown(DestReceiver *self __attribute__((unused)))
{
	/* no-op */
}

/*
 * copy_dest_destroy --- release DestReceiver object
 */
static void
copy_dest_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * CreateCopyDestReceiver -- create a suitable DestReceiver object
 */
DestReceiver *
CreateCopyDestReceiver(void)
{
	DR_copy    *self = (DR_copy *) palloc(sizeof(DR_copy));

	self->pub.receiveSlot = copy_dest_receive;
	self->pub.rStartup = copy_dest_startup;
	self->pub.rShutdown = copy_dest_shutdown;
	self->pub.rDestroy = copy_dest_destroy;
	self->pub.mydest = DestCopyOut;

	self->cstate = NULL;		/* will be set later */

	return (DestReceiver *) self;
}


static void CopyInitPartitioningState(EState *estate)
{
	if (estate->es_result_partitions)
	{
		estate->es_partition_state =
 			createPartitionState(estate->es_result_partitions,
								 estate->es_num_result_relations);
	}
}

/*
 * Initialize data loader parsing state
 */
static void CopyInitDataParser(CopyState cstate)
{
	cstate->fe_eof = false;
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->null_print_len = strlen(cstate->null_print);

	if (cstate->csv_mode)
	{
		cstate->in_quote = false;
		cstate->last_was_esc = false;
		cstate->num_consec_csv_err = 0;
	}

	/* Set up data buffer to hold a chunk of data */
	MemSet(cstate->raw_buf, ' ', RAW_BUF_SIZE * sizeof(char));
	cstate->raw_buf[RAW_BUF_SIZE] = '\0';
	cstate->line_done = true;
	cstate->raw_buf_done = false;
}

/*
 * CopyCheckIsLastLine
 *
 * This routine checks if the line being looked at is the last line of data.
 * If it is, it makes sure that this line is terminated with an EOL. We must
 * do this check in order to support files that don't end up EOL before EOF,
 * because we want to treat that last line as normal - and be able to pre
 * process it like the other lines (remove metadata chars, encoding conversion).
 *
 * See MPP-4406 for an example of why this is needed.
 */
static bool CopyCheckIsLastLine(CopyState cstate)
{
	if (cstate->fe_eof)
	{
		concatenateEol(cstate);
		return true;
	}
	
	return false;
}

/*
 * setEncodingConversionProc
 *
 * COPY and External tables use a custom path to the encoding conversion
 * API because external tables have their own encoding (which is not
 * necessarily client_encoding). We therefore have to set the correct
 * encoding conversion function pointer ourselves, to be later used in
 * the conversion engine.
 *
 * The code here mimics a part of SetClientEncoding() in mbutils.c
 */
void setEncodingConversionProc(CopyState cstate, int client_encoding, bool iswritable)
{
	Oid		conversion_proc;
	
	/*
	 * COPY FROM and RET: convert from client to server
	 * COPY TO   and WET: convert from server to client
	 */
	if (iswritable)
		conversion_proc = FindDefaultConversionProc(GetDatabaseEncoding(),
													client_encoding);
	else		
		conversion_proc = FindDefaultConversionProc(client_encoding,
												    GetDatabaseEncoding());
	
	if (OidIsValid(conversion_proc))
	{
		/* conversion proc found */
		cstate->enc_conversion_proc = palloc(sizeof(FmgrInfo));
		fmgr_info(conversion_proc, cstate->enc_conversion_proc);
	}
	else
	{
		/* no conversion function (both encodings are probably the same) */
		cstate->enc_conversion_proc = NULL;
	}
}

/*
 * preProcessDataLine
 *
 * When Done reading a complete data line set input row number for error report
 * purposes (this also removes any metadata that was concatenated to the data
 * by the QD during COPY) and convert it to server encoding if transcoding is
 * needed.
 */
static
void preProcessDataLine(CopyState cstate)
{
	char	   *cvt;
	bool		force_transcoding = false;

	/*
	 * Increment line count by 1 if we have access to all the original
	 * data rows and can count them reliably (ROWNUM_ORIGINAL). However
	 * if we have ROWNUM_EMBEDDED the original row number for this row
	 * was sent to us with the data (courtesy of the data distributor), so
	 * get that number instead.
	 */
	if(cstate->err_loc_type == ROWNUM_ORIGINAL)
	{
		cstate->cur_lineno++;
	}
	else if(cstate->err_loc_type == ROWNUM_EMBEDDED)
	{
		Assert(Gp_role == GP_ROLE_EXECUTE);
		
		/*
		 * Extract various metadata sent to us from the QD COPY about this row:
		 * 1) the original line number of the row.
		 * 2) if the row was converted to server encoding or not
		 */
		CopyExtractRowMetaData(cstate); /* sets cur_lineno internally */
		
		/* check if QD sent us a badly encoded row, still in client_encoding, 
		 * in order to catch the encoding error ourselves. if line_buf_converted
		 * is false after CopyExtractRowMetaData then we must transcode and catch
		 * the error. Verify that we are indeed in SREH error table mode. that's
		 * the only valid path for receiving an unconverted data row.
		 */
		if (!cstate->line_buf_converted)
		{
			Assert(cstate->errMode == SREH_LOG);
			force_transcoding = true; 
		}
			
	}
	else
	{
		Assert(false); /* byte offset not yet supported */
	}
	
	if (cstate->need_transcoding || force_transcoding)
	{
		cvt = (char *) pg_custom_to_server(cstate->line_buf.data,
										   cstate->line_buf.len,
										   cstate->client_encoding,
										   cstate->enc_conversion_proc);
		
		Assert(!force_transcoding); /* if force is 't' we must have failed in the conversion */
		
		if (cvt != cstate->line_buf.data)
		{
			/* transfer converted data back to line_buf */
			RESET_LINEBUF;
			appendBinaryStringInfo(&cstate->line_buf, cvt, strlen(cvt));
			pfree(cvt);
		}
	}
	/* indicate that line buf is in server encoding */
	cstate->line_buf_converted = true;
}

void CopyEolStrToType(CopyState cstate)
{
	if (pg_strcasecmp(cstate->eol_str, "lf") == 0)
	{
		cstate->eol_type = EOL_LF;
		cstate->eol_ch[0] = '\n';
		cstate->eol_ch[1] = '\0';
	}
	else if (pg_strcasecmp(cstate->eol_str, "cr") == 0)
	{
		cstate->eol_type = EOL_CR;
		cstate->eol_ch[0] = '\r';
		cstate->eol_ch[1] = '\0';		
	}
	else if (pg_strcasecmp(cstate->eol_str, "crlf") == 0)
	{
		cstate->eol_type = EOL_CRLF;
		cstate->eol_ch[0] = '\r';
		cstate->eol_ch[1] = '\n';		
		
	}
	else /* error. must have been validated in CopyValidateControlChars() ! */
		ereport(ERROR,
				(errcode(ERRCODE_CDB_INTERNAL_ERROR),
				 errmsg("internal error in CopySetEolType. Trying to set NEWLINE %s", 
						 cstate->eol_str)));
}
