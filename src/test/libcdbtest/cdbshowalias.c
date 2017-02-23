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
 * cdbshowalias.c
 *
 */
#include "postgres.h"

#include <ctype.h>
#include <assert.h>
#include "tcop/tcopprot.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "nodes/pg_list.h"

#include "cdbshowalias.h"

/* general utility */
#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))


/*
 * cdb_show_alias takes 1 argument:
 *  1) an SQL statement
 * It returns a string which is an alias definition for the result set of the SQL statement.
 */
PG_FUNCTION_INFO_V1(cdb_show_alias);
Datum
cdb_show_alias(PG_FUNCTION_ARGS)
{
	char			*pszSQL = GET_STR(PG_GETARG_TEXT_P(0));
	int				i;
	StringInfoData	buffer;


	initStringInfo( &buffer );

	/* Get tuple desc that will be the result of executing this
	 * sql statement 
	 * Step 1: create a parse tree from the sql statement.
	 *			Validate that there is only 1.
	 * Step 2: create a query tree from the parse tree.
	 *			Validate that it is a select statement.
	 * Step 3: get the type for each of the result columns.
	 */
    List	*raw_parsetree_list;

	/*
	 * Here we get the parse tree for the sql statement
	 */
	raw_parsetree_list = pg_parse_query(pszSQL);
	
	/*
	 *  Validate that there was only 1 statement
	 */
	if ( list_length(raw_parsetree_list) != 1 )
	{
        ereport(ERROR,
                (errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH),
                    errmsg("Function cdb_show_alias() cannot process multiple statements."))
                );
	}

	/*
	 *  This gets the query tree from the parse tree.
	 */
	Node *parsetree = (Node *)linitial(raw_parsetree_list);
	List *query_list = pg_analyze_and_rewrite( parsetree, NULL, 0);
	
	/*
	 * Validate that there is only one query tree
	 */
	if ( list_length(query_list) != 1 )
	{
        ereport(ERROR,
                (errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH),
                    errmsg("Function cdb_show_alias() cannot process multiple statements."))
                );
	}
		
	Query *queryTree = (Query *)linitial(query_list);
	
	// Make sure that this is a select statement
	if ( queryTree->type != T_Query || 
		queryTree->commandType != CMD_SELECT )
	{
        ereport(ERROR,
                (errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH),
                    errmsg("Function cdb_show_alias() can only process SELECT statements."))
                );
	}
	
	List *targetList = queryTree->targetList;

	i = 0;

	appendStringInfo(&buffer, "as T(");

	
	/*
	 * Follow the linked list of TargetEntry's from the query tree's targetList
	 * Then check the resdom, and if it's a true result column,
	 * get the typeoid and typemod
	 * and compare these to the same fields from the attrs array
	 */

	/*
	 * To make the alias column names unique is tricky.
	 * The tr->resname are not necessarily unique.
	 * We use an ID, called disambiguatingID, to insure uniqueness.
	 * We save the current column names in a sorted array called NamesAr.
	 * When we get to a new column, if the resname matches one we already have,
	 * we append the disambiguatingID to it and try again.  Finally, when we get a unique
	 * name, we add that to its proper place in the sortted NamesAr.
	 */
	int disambiguatingID = 1;

	char	**NamesAr = palloc0(sizeof(char *) * list_length(targetList) );
	int		countNames = 0;

	ListCell *lc;
	foreach( lc, targetList )
	{
		TargetEntry	*target;
		Resdom		*tr;

		target = (TargetEntry *)(lfirst(lc));
		tr = target->resdom;

		if ( !tr->resjunk )
		{
			if ( i > 0 )
				appendStringInfo(&buffer, ", ");

			StringInfoData nameStr;
			initStringInfo( &nameStr );
			appendStringInfo( &nameStr, tr->resname );
			int cmp;
			int j;

			while ( true )
			{
				for ( j=0; j<countNames; j++ )
				{
					cmp = strcmp(NamesAr[j], nameStr.data);
					if ( cmp == 0 )
					{
						pfree(nameStr.data);
						initStringInfo( &nameStr );
						appendStringInfo( &nameStr, "%s%d", tr->resname, disambiguatingID++ );
						break;
					}

					if ( cmp > 0 )
					{
						break;
					}
				}

				if ( cmp != 0 )
					break;
			}

			// Insert at location j
			int k;
			for ( k=countNames-1; k>=j; k--)
			{
				NamesAr[k+1] = NamesAr[k];
			}

			NamesAr[j] = pstrdup(nameStr.data);
			countNames++;


			appendStringInfo(&buffer, "%s %s", 
							nameStr.data, 
							format_type_be(tr->restype));
			
			pfree(nameStr.data);
			
			i++;
		}
	}

	for ( i=0; i<countNames; i++)
		pfree(NamesAr[i]);
	pfree(NamesAr);
	
	appendStringInfo(&buffer, ")");

	int lenOutput = strlen(buffer.data);
	int len = lenOutput + VARHDRSZ;
	text* result = (text *) palloc(len);

	/* Set size of result string... */
	SET_VARSIZE(result, len);

	/* Fill data field of result string... */
	char* ptr = VARDATA(result);
	memcpy(ptr, buffer.data, lenOutput);

	pfree(pszSQL);
	pfree(buffer.data);

	PG_RETURN_TEXT_P(result);
}

