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
 * ---------------------------------------------------------------------
 *
 * The dynamically linked library created from this source can be reference by
 * creating a function in psql that references it. For example,
 *
 * CREATE FUNCTION gp_dump_query_oids(text)
 *	RETURNS text
 *	AS '$libdir/gpoptutils', 'gp_dump_query_oids'
 *	LANGUAGE C STRICT;
 */

#include "postgres_fe.h"
#include "postgres.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/tcopprot.h"

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

Datum gp_dump_query_oids(PG_FUNCTION_ARGS);

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(gp_dump_query_oids);

static void
traverseQueryOids
	(
	Query          *pquery,
	HTAB           *relhtab,
	StringInfoData *relbuf,
	HTAB           *funchtab,
	StringInfoData *funcbuf
	)
{
	bool	   found;
	const char *whitespace = " \t\n\r";
	char	   *query = nodeToString(pquery);
	char	   *token = strtok(query, whitespace);

	while (token)
	{
		if (pg_strcasecmp(token, ":relid") == 0)
		{
			token = strtok(NULL, whitespace);
			if (token)
			{
				Oid relid = atooid(token);
				hash_search(relhtab, (void *)&relid, HASH_ENTER, &found);
				if (!found)
				{
					if (relbuf->len != 0)
						appendStringInfo(relbuf, "%s", ",");
					appendStringInfo(relbuf, "%u", relid);
				}
			}
		}
		else if (pg_strcasecmp(token, ":funcid") == 0)
		{
			token = strtok(NULL, whitespace);
			if (token)
			{
				Oid funcid = atooid(token);
				hash_search(funchtab, (void *)&funcid, HASH_ENTER, &found);
				if (!found)
				{
					if (funcbuf->len != 0)
						appendStringInfo(funcbuf, "%s", ",");
					appendStringInfo(funcbuf, "%u", funcid);
				}
			}
		}

		token = strtok(NULL, whitespace);
	}
}

/*
 * Function dumping dependent relation & function oids for a given SQL text
 */
Datum
gp_dump_query_oids(PG_FUNCTION_ARGS)
{
	char *sqlText = text_to_cstring(PG_GETARG_TEXT_P(0));
	List *queryList = pg_parse_and_rewrite(sqlText, NULL, 0);
	ListCell *plc;

	StringInfoData relbuf, funcbuf;
	initStringInfo(&relbuf);
	initStringInfo(&funcbuf);

	typedef struct OidHashEntry
	{
		Oid key;
		bool value;
	} OidHashEntry;
	HASHCTL ctl;
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(OidHashEntry);
	ctl.hash = oid_hash;
	HTAB *relhtab = hash_create("relid hash table", 100, &ctl, HASH_ELEM | HASH_FUNCTION);
	HTAB *funchtab = hash_create("funcid hash table", 100, &ctl, HASH_ELEM | HASH_FUNCTION);

	foreach(plc, queryList)
	{
		Query *query = (Query *) lfirst(plc);
		if (CMD_UTILITY == query->commandType && T_ExplainStmt == query->utilityStmt->type)
		{
			Query *queryExplain = ((ExplainStmt *)query->utilityStmt)->query;
			List *queryTree = QueryRewrite(queryExplain);
			Assert(1 == list_length(queryTree));
			query = (Query *) lfirst(list_head(queryTree));
		}
		traverseQueryOids(query, relhtab, &relbuf, funchtab, &funcbuf);
	}

	hash_destroy(relhtab);
	hash_destroy(funchtab);

	StringInfoData str;
	initStringInfo(&str);
	appendStringInfo(&str, "{\"relids\": \"%s\", \"funcids\": \"%s\"}", relbuf.data, funcbuf.data);

	text *result = cstring_to_text(str.data);
	pfree(relbuf.data);
	pfree(funcbuf.data);
	pfree(str.data);

	PG_RETURN_TEXT_P(result);
}
