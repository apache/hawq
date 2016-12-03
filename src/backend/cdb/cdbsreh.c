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

/*--------------------------------------------------------------------------
*
* cdbsreh.c
*	  Provides routines for single row error handling for COPY and external
*	  tables.
*
*--------------------------------------------------------------------------
*/
#include "postgres.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/acl.h"
#include "miscadmin.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/tablecmds.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/uri.h"
#include "nodes/makefuncs.h"
#include "access/transam.h"

#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"

static void OpenErrorTable(CdbSreh *cdbsreh, RangeVar *errortable);
static void InsertIntoErrorTable(CdbSreh *cdbsreh);
static void CloseErrorTable(CdbSreh *cdbsreh);
static void DropErrorTable(CdbSreh *cdbsreh);
static int  GetNextSegid(CdbSreh *cdbsreh);
static void VerifyErrorTableAttr(Form_pg_attribute *attr, 
								 int attrnum, 
								 const char *expected_attname,
								 Oid expected_atttype,
								 char *relname);
static void PreprocessByteaData(char *src);


/*
 * makeCdbSreh
 *
 * Allocate and initialize a Single Row Error Handling state object.
 * Pass in the only known parameters (both we get from the SQL stmt),
 * the other variables are set later on, when they are known.
 */
CdbSreh *
makeCdbSreh(bool is_keep, bool reusing_existing_errtable,
			int rejectlimit, bool is_limit_in_rows,
			RangeVar *errortable, ResultRelSegFileInfo *segfileinfo, char *filename, char *relname)
{
	CdbSreh	*h;

	h = palloc(sizeof(CdbSreh));
	
	h->errmsg = NULL;
	h->rawdata = NULL;
	h->linenumber = 0;
	h->processed = 0;
	h->relname = relname;
	h->rejectlimit = rejectlimit;
	h->is_limit_in_rows = is_limit_in_rows;
	h->rejectcount = 0;
	h->is_server_enc = false;
	h->is_keep = is_keep;
	h->should_drop = false; /* we'll decide later */
	h->reusing_errtbl = reusing_existing_errtable;
	h->cdbcopy = NULL;
	h->errtbl = NULL;
	h->err_aoInsertDesc = NULL;
	if (segfileinfo != NULL)
	{
		h->err_aosegno = segfileinfo->segno;
	}
	else
	{
		h->err_aosegno = 0;
	}
	h->err_aosegfileinfo = segfileinfo;
	h->lastsegid = 0;
	h->consec_csv_err = 0;
	
	snprintf(h->filename, sizeof(h->filename), "%s", filename ? CleanseUriString(filename) : "<stdin>");
	
	/* error table was specified open it (and create it first if necessary) */
	if(errortable)
		OpenErrorTable(h, errortable);
	
	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype input routines, and should be faster than retail pfree's
	 * anyway.
	 */
	h->badrowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "SrehMemCtxt",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
	
	return h;
}

void
destroyCdbSreh(CdbSreh *cdbsreh)
{
	
	if (cdbsreh->err_aoInsertDesc)
	{
		StringInfo buf = NULL;
		buf = PreSendbackChangedCatalog(1);

		QueryContextDispatchingSendBack sendback = NULL;

		sendback = CreateQueryContextDispatchingSendBack(1);
		cdbsreh->err_aoInsertDesc->sendback = sendback;
		sendback->relid = RelationGetRelid(cdbsreh->errtbl);

		appendonly_insert_finish(cdbsreh->err_aoInsertDesc);

		if (sendback && Gp_role == GP_ROLE_EXECUTE)
			AddSendbackChangedCatalogContent(buf, sendback);

		DropQueryContextDispatchingSendBack(sendback);

		if (sendback && Gp_role == GP_ROLE_EXECUTE)
			FinishSendbackChangedCatalog(buf);
	}

	/* delete the bad row context */
	MemoryContextDelete(cdbsreh->badrowcontext);

	/* close error table */
	if (cdbsreh->errtbl)
		CloseErrorTable(cdbsreh);

	/* drop error table if need to */
	if (cdbsreh->should_drop && Gp_role == GP_ROLE_DISPATCH)
		DropErrorTable(cdbsreh);
	
	pfree(cdbsreh);
}

/*
 * HandleSingleRowError
 *
 * The single row error handler. Gets called when a data error happened
 * in SREH mode. Reponsible for making a decision of what to do at that time.
 *
 * Some of the main actions are:
 *  - Keep track of reject limit. if reached make sure notify caller.
 *  - If error table used, call the error logger to log this error.
 *  - If QD COPY send the bad row to the QE COPY to deal with.
 *
 */
void HandleSingleRowError(CdbSreh *cdbsreh)
{
	
	/* increment total number of errors for this segment */ 
	cdbsreh->rejectcount++;
	
	/* 
	 * if reached the segment reject limit don't do anything.
	 * (this will get checked and handled later on by the caller).
	 */
	if(IsRejectLimitReached(cdbsreh))
		return;
	
	/*
	 * If not using an error table, we don't need to do anything.
	 *
	 * However, if we use an error table:
	 * QE - log the error in the error table.
	 * QD - send the bad data row to a random QE (via roundrobin).
	 */
	if(cdbsreh->errtbl)
	{
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			cdbCopySendData(cdbsreh->cdbcopy, 
							GetNextSegid(cdbsreh), 
							cdbsreh->rawdata, 
							strlen(cdbsreh->rawdata));
			
		}
		else
		{
			/* Insert into error table */
			Insist(Gp_role == GP_ROLE_EXECUTE || Gp_role == GP_ROLE_UTILITY);
			InsertIntoErrorTable(cdbsreh);
		}
		
	}
	
	return; /* OK */
}

/*
 * OpenErrorTable
 *
 * Open the error table for this operation, and perform all necessary checks.
 */
void OpenErrorTable(CdbSreh *cdbsreh, RangeVar *errortable)
{
	AclResult		aclresult;
	AclMode			required_access = ACL_INSERT;
	Oid				relOid;


	/* Open and lock the error relation, using the appropriate lock type. */
	cdbsreh->errtbl = heap_openrv(errortable, RowExclusiveLock);
	
	relOid = RelationGetRelid(cdbsreh->errtbl);
	
	/* Check relation permissions. */
	aclresult = pg_class_aclcheck(relOid,
								  GetUserId(),
								  required_access);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_CLASS,
					   RelationGetRelationName(cdbsreh->errtbl));
	
	/* check read-only transaction */
	if (XactReadOnly &&
		!isTempNamespace(RelationGetNamespace(cdbsreh->errtbl)))
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("transaction is read-only")));
	
	/* make sure this is a regular relation */
	if (cdbsreh->errtbl->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" exists in the database but is a non table relation",
						RelationGetRelationName(cdbsreh->errtbl))));

	if (!RelationIsAoRows(cdbsreh->errtbl))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" exists in the database but is a non appendonly relation",
						RelationGetRelationName(cdbsreh->errtbl))));
}

/*
 * CloseErrorTable
 *
 * Done using the error table. Close it.
 */
void CloseErrorTable(CdbSreh *cdbsreh)
{
	/* close the error relation */
	if (cdbsreh->errtbl)
		heap_close(cdbsreh->errtbl, NoLock);
}

/*
 * DropErrorTable
 *
 * Drop the error table from the database. This function will be called from
 * destroyCdbSreh when an autogenerated error table was not used in the COPY
 * operation granted KEEP wasn't specified.
 *
 */
static
void DropErrorTable(CdbSreh *cdbsreh)
{
	RangeVar *errtbl_rv;
	
	Insist(Gp_role == GP_ROLE_DISPATCH);

	ereport(NOTICE,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("Dropping the auto-generated unused error table"),
			 errhint("Use KEEP in LOG INTO clause to force keeping the error table alive")));								
	
	errtbl_rv = makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(cdbsreh->errtbl)),
							 RelationGetRelationName(cdbsreh->errtbl), -1);

	/* DROP the relation on the QD */
	RemoveRelation(errtbl_rv,DROP_RESTRICT, NULL);
}

/*
 * InsertIntoErrorTable
 *
 * Insert the information in cdbsreh into the error table we are using.
 */
void InsertIntoErrorTable(CdbSreh *cdbsreh)
{
	MemTuple	tuple;
	Oid			tupleOid;
	AOTupleId	aoTupleId;
	bool		nulls[NUM_ERRORTABLE_ATTR];
	Datum		values[NUM_ERRORTABLE_ATTR];
	MemoryContext oldcontext;
					
	oldcontext = MemoryContextSwitchTo(cdbsreh->badrowcontext);
	
	/* Initialize all values for row to NULL */
	MemSet(values, 0, NUM_ERRORTABLE_ATTR * sizeof(Datum));
	MemSet(nulls, true, NUM_ERRORTABLE_ATTR * sizeof(bool));
	
	/* command start time */
	values[errtable_cmdtime - 1] = TimestampTzGetDatum(GetCurrentStatementStartTimestamp());
	nulls[errtable_cmdtime - 1] = false;
		
	/* line number */
	if (cdbsreh->linenumber > 0)
	{
		values[errtable_linenum - 1] = Int64GetDatum(cdbsreh->linenumber);
		nulls[errtable_linenum - 1] = false;
	}

	if(cdbsreh->is_server_enc)
	{
		/* raw data */
		values[errtable_rawdata - 1] = DirectFunctionCall1(textin, CStringGetDatum(cdbsreh->rawdata));
		nulls[errtable_rawdata - 1] = false;
	}
	else
	{
		/* raw bytes */
		PreprocessByteaData(cdbsreh->rawdata);
		values[errtable_rawbytes - 1] = DirectFunctionCall1(byteain, CStringGetDatum(cdbsreh->rawdata));
		nulls[errtable_rawbytes - 1] = false;
	}
	
	/* file name */
	values[errtable_filename - 1] = DirectFunctionCall1(textin, CStringGetDatum(cdbsreh->filename));
	nulls[errtable_filename - 1] = false;

	/* relation name */
	values[errtable_relname - 1] = DirectFunctionCall1(textin, CStringGetDatum(cdbsreh->relname));
	nulls[errtable_relname - 1] = false;
	
	/* error message */
	values[errtable_errmsg - 1] = DirectFunctionCall1(textin, CStringGetDatum(cdbsreh->errmsg));
	nulls[errtable_errmsg - 1] = false;
	
	MemoryContextSwitchTo(oldcontext);

	/*
	 * cdbsreh->err_aoInsertDesc should be available untile the end of statement.
	 */
	oldcontext = MemoryContextSwitchTo(PortalContext);

	if (cdbsreh->err_aoInsertDesc == NULL)
		cdbsreh->err_aoInsertDesc = appendonly_insert_init(cdbsreh->errtbl,
				cdbsreh->err_aosegfileinfo);

	MemoryContextSwitchTo(oldcontext);

	/* form a mem tuple */
	tuple = memtuple_form_to(cdbsreh->err_aoInsertDesc->mt_bind, values, nulls,	NULL, NULL, true);

	/* inserting into an append only relation */
	appendonly_insert(cdbsreh->err_aoInsertDesc, tuple, &tupleOid, &aoTupleId);

}


/* 
 * ReportSrehResults
 *
 * When necessary emit a NOTICE that describes the end result of the
 * SREH operations. Information includes the total number of rejected
 * rows, and whether rows were ignored or logged into an error table.
 */
void ReportSrehResults(CdbSreh *cdbsreh, int total_rejected)
{
	if(total_rejected > 0)
	{
		if(cdbsreh && cdbsreh->errtbl)
			ereport(NOTICE, 
					(errmsg("Found %d data formatting errors (%d or more "
							"input rows). Errors logged into error table \"%s\"", 
							total_rejected, total_rejected,
							RelationGetRelationName(cdbsreh->errtbl))));
		else
			ereport(NOTICE, 
					(errmsg("Found %d data formatting errors (%d or more "
							"input rows). Rejected related input data.", 
							total_rejected, total_rejected)));
	}
}


/*
 * SendNumRowsRejected
 *
 * Using this function the QE sends back to the client QD the number 
 * of rows that were rejected in this last data load in SREH mode.
 */
void SendNumRowsRejected(int numrejected)
{
	StringInfoData buf;
	
	if (Gp_role != GP_ROLE_EXECUTE)
		elog(FATAL, "SendNumRowsRejected: called outside of execute context.");

	pq_beginmessage(&buf, 'j'); /* 'j' is the msg code for rejected records */
	pq_sendint(&buf, numrejected, 4);
	pq_endmessage(&buf);	
}

/*
 * ValidateErrorTableMetaData
 *
 * This function gets called if a user wants an already existing table to be
 * used as an error table for some COPY or external table operation with SREH.
 * In here we verify that the metadata of the user selected table matches the
 * predefined requirement for an error table.
 */
void ValidateErrorTableMetaData(Relation rel)
{
	TupleDesc   tupDesc = RelationGetDescr(rel);
	Form_pg_attribute *attr = tupDesc->attrs;
	TupleConstr *constr = tupDesc->constr;
	char		*relname = RelationGetRelationName(rel);
	int			attr_count = tupDesc->natts;
	
	/*
	 * Verify number of attributes match
	 */
	if(attr_count != NUM_ERRORTABLE_ATTR)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("Relation \"%s\" already exists and is not of a valid "
						"error table format (expected %d attributes, found %d)",
						relname, NUM_ERRORTABLE_ATTR, attr_count)));

	/*
	 * Verify this table has no constraints
	 *
	 * (TODO: this currently checks for all the contraints in TupleConstr
	 *        which are, defaults, check, and not null. We need to check 
	 *		  for unique constraints as well).
	 */
	if(constr)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("Relation \"%s\" already exists and is not of a valid "
						"error table format. It appears to have constraints "
						"defined.", relname)));

	if (!RelationIsAoRows(rel))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("Relation \"%s\" already exists and is not of a valid "
							"error table format. It appears to not a appendonly table", relname)));

	if (rel->rd_cdbpolicy == NULL
			|| rel->rd_cdbpolicy->ptype != POLICYTYPE_PARTITIONED
			|| rel->rd_cdbpolicy->nattrs != 0)
		ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("Relation \"%s\" already exists and is not of a valid "
							"error table format. It appears to not distributed randomly", relname)));


	/*
	 * run through each attribute at a time and verify it's what we expect
	 */
	VerifyErrorTableAttr(attr, errtable_cmdtime, "cmdtime", TIMESTAMPTZOID, relname);
	VerifyErrorTableAttr(attr, errtable_relname, "relname", TEXTOID, relname);
	VerifyErrorTableAttr(attr, errtable_filename, "filename", TEXTOID, relname);
	VerifyErrorTableAttr(attr, errtable_linenum, "linenum", INT4OID, relname);
	VerifyErrorTableAttr(attr, errtable_bytenum, "bytenum", INT4OID, relname);
	VerifyErrorTableAttr(attr, errtable_errmsg, "errmsg", TEXTOID, relname);
	VerifyErrorTableAttr(attr, errtable_rawdata, "rawdata", TEXTOID, relname);
	VerifyErrorTableAttr(attr, errtable_rawbytes, "rawbytes", BYTEAOID, relname);
	
}

/*
 * VerifyErrorTableAttr
 *
 * Called by ValidateErrorTableMetaData() on each table attribute to verify
 * that it has the right predefined name, type and other attr characteristics.
 */
void VerifyErrorTableAttr(Form_pg_attribute *attr, 
						  int attrnum, 
						  const char *expected_attname,
						  Oid expected_atttype,
						  char *relname)
{
	
	bool		cur_attisdropped = attr[attrnum - 1]->attisdropped;
	Name		cur_attname = &(attr[attrnum - 1]->attname);
	Oid			cur_atttype = attr[attrnum - 1]->atttypid;
	int4		cur_attndims = attr[attrnum - 1]->attndims;
		
	if(cur_attisdropped)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("Relation \"%s\" includes dropped attributes and is "
						"therefore not of a valid error table format",
						relname)));

	if (namestrcmp(cur_attname, expected_attname) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("Relation \"%s\" is an invalid error table. Expected "
						"attribute \"%s\" found \"%s\"",
						relname, expected_attname, NameStr(*cur_attname))));
	
	if(cur_atttype != expected_atttype)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("Relation \"%s\" is an invalid error table. Wrong data "
						"type for attribute \"%s\"",
						relname, NameStr(*cur_attname))));
	
	if(cur_attndims > 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("Relation \"%s\" is an invalid error table. Attribute "
						"\"%s\" has more than zero dimensions (array).",
						relname, NameStr(*cur_attname))));
	
	
}

/*
 * SetErrorTableVerdict
 *
 * Do we kill (DROP) the error table or let it live? This function is called 
 * at the end of execution of a COPY operation involving an error table. The
 * rule is - always keep it alive if KEEP was specified in the COPY command
 * otherwise DROP it only if no rows were rejected *and* if it was auto
 * generated at the start of this COPY command (did not exist before).
 */
void
SetErrorTableVerdict(CdbSreh *cdbsreh, int total_rejected)
{
	/* never let a QE decide whether to drop the table or not */
	Insist(Gp_role == GP_ROLE_DISPATCH);
	
	/* this is for COPY use only, don't call this function for external tables */
	Insist(cdbsreh->cdbcopy);
		   
	/* we can't get here if we don't have an open error table */
	Insist(cdbsreh->errtbl);
	
	if(!cdbsreh->is_keep && !cdbsreh->reusing_errtbl && total_rejected == 0)
	{
		cdbsreh->should_drop = true; /* mark this table to be dropped */
	}
}

/*
 * IsRejectLimitReached
 *
 * Returns true if seg reject limit reached, false otherwise.
 */
bool IsRejectLimitReached(CdbSreh *cdbsreh)
{
	bool	limit_reached = false;
	
	/* special case: check for un-parsable csv format errors */
	if(CSV_IS_UNPARSABLE(cdbsreh))
		return true;
	
	/* now check if actual reject limit is reached */
	if(cdbsreh->is_limit_in_rows)
	{
		/* limit is in ROWS */
		
		limit_reached = (cdbsreh->rejectcount >= cdbsreh->rejectlimit ? true : false);
	}
	else
	{
		/* limit is in PERCENT */
		
		/* calculate the percent only if threshold is satisfied */
		if(cdbsreh->processed > gp_reject_percent_threshold)
		{
			if( (cdbsreh->rejectcount * 100) / cdbsreh->processed >= cdbsreh->rejectlimit)
				limit_reached = true;
		}
	}

	return limit_reached;
}

/*
 * emitSameTxnWarning()
 *
 * Warn the user that it's better to have the error table created in a
 * transaction that is older than the one that is using it. This mostly
 * can happen in COPY but also in some not so common external table use
 * cases (BEGIN, CREATE EXTERNAL TABLE with errtable, SELECT..., ...).
 */
void emitSameTxnWarning(void)
{	
	ereport(WARNING, 
			(errcode(ERRCODE_T_R_GP_ERROR_TABLE_MAY_DROP),
			 errmsg("The error table was created in the same "
					"transaction as this operation. It will get "
					"dropped if transaction rolls back even if bad "
					"rows are present"),
			 errhint("To avoid this create the error table ahead "
					"of time using: CREATE TABLE <name> (cmdtime "
					"timestamp with time zone, relname text, "
					"filename text, linenum integer, bytenum "
					"integer, errmsg text, rawdata text, rawbytes "
					"bytea)")));
}
/*
 * GetNextSegid
 *
 * Return the next sequential segment id of available segids (roundrobin).
 */
static
int GetNextSegid(CdbSreh *cdbsreh)
{
	int total_segs = cdbsreh->cdbcopy->partition_num;
	
	if(cdbsreh->lastsegid == total_segs)
		cdbsreh->lastsegid = 0; /* start over from first segid */
	
	return (cdbsreh->lastsegid++ % total_segs);
}


/*
 * This function is called when we are preparing to insert a bad row that
 * includes an encoding error into the bytea field of the error table 
 * (rawbytes). In rare occasions this bad row may also have an invalid bytea
 * sequence - a backslash not followed by a valid octal sequence - in which
 * case inserting into the error table will fail. In here we make a pass to
 * detect if there's a risk of failing. If there isn't we just return. If there
 * is we remove the backslash and replace it with a x20 char. Yes, we are
 * actually modifying the user data, but this is a much better opion than
 * failing the entire load. It's also a bad row - a row that will require user
 * intervention anyway in order to reload.
 *
 * reference: MPP-2107
 *
 * NOTE: code is copied from esc_dec_len() in encode.c and slightly modified.
 */
static
void PreprocessByteaData(char *src)
{
	const char *end = src + strlen(src);
	
	while (src < end)
	{
		if (src[0] != '\\')
			src++;
		else if (src + 3 < end &&
				 (src[1] >= '0' && src[1] <= '3') &&
				 (src[2] >= '0' && src[2] <= '7') &&
				 (src[3] >= '0' && src[3] <= '7'))
		{
			/*
			 * backslash + valid octal
			 */
			src += 4;
		}
		else if (src + 1 < end &&
				 (src[1] == '\\'))
		{
			/*
			 * two backslashes = backslash
			 */
			src += 2;
		}
		else
		{
			/*
			 * one backslash, not followed by ### valid octal. remove the
			 * backslash and put a x20 in its place.
			 */
			src[0] = ' ';
			src++;
		}		
	}
	
}

/*
 * IsRejectLimitValid
 * 
 * verify that the the reject limit specified by the user is within the
 * allowed values for ROWS or PERCENT.
 */
void VerifyRejectLimit(char rejectlimittype, int rejectlimit)
{
	if(rejectlimittype == 'r')
	{
		/* ROWS */
		if(rejectlimit < 2)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("Segment reject limit in ROWS "
							"must be 2 or larger (got %d)", rejectlimit)));
	}
	else
	{
		/* PERCENT */
		Assert(rejectlimittype == 'p');
		if (rejectlimit < 1 || rejectlimit > 100)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("Segment reject limit in PERCENT "
							"must be between 1 and 100 (got %d)", rejectlimit)));
	}

}
