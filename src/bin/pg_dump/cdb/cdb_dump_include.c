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
 * cdb_dump_include.c
 *	  cdb_dump_include is collecion of functions used to determine the
 *	  set of tables to be included in a dump.  These functions are
 *	  largely extracted from pg_dump.c.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pg_backup.h"
#include "catalog/pg_class.h"
#include "dumputils.h"

#include "cdb_dump_util.h"
#include "cdb_dump_include.h"

static NamespaceInfo *findNamespace(Oid nsoid, Oid objoid);
static void getDomainConstraints(TypeInfo *tinfo);

static void expand_schema_name_patterns(SimpleStringList *patterns,
							SimpleOidList *oids);
static void expand_table_name_patterns(SimpleStringList *patterns,
						   SimpleOidList *oids);


/*
 * Subquery used to convert user ID (eg, datdba) to user name. The default
 * value is "SELECT rolname FROM pg_catalog.pg_roles WHERE oid =" but may
 * be set to a string of similar form.
 */
const char *username_subquery = "SELECT rolname FROM pg_catalog.pg_roles WHERE oid =";

/*
 * String designating the mode used to LOCK the tables to be dumped.
 */
const char *table_lock_mode = "ACCESS SHARE";

/*
 * Partitioning is presumed supported by this code level.  The caller
 * of cdb_dump_include functions may declare otherwise.
 */
bool		g_gp_supportsPartitioning = true;

/*
 * MPP-6095: Partition templates are presumed supported by this code
 * level.  The caller of cdb_dump_include functions may declare
 * otherwise.
 */
bool		g_gp_supportsPartitionTemplates = true;

/*
 * Indicates whether or not the GPDB cluster supports column attributes.
 */
bool g_gp_supportsAttributeEncoding = false;


/*
 * When true, indicates that only the MPP schema is being dumped.
 */
bool		g_gp_catalog_only = false;

const CatalogId nilCatalogId = {0, 0};


/*
 * Constants used for message logging.
 */
static const char *logInfo = "INFO";
static const char *logWarn = "WARN";
static const char *logError = "ERROR";


/*
 * Object inclusion/exclusion lists
 *
 * The string lists record the patterns given by command-line switches,
 * which we then convert to lists of OIDs of matching objects.
 */
SimpleStringList schema_include_patterns = {NULL, NULL};
SimpleOidList schema_include_oids = {NULL, NULL};
SimpleStringList schema_exclude_patterns = {NULL, NULL};
SimpleOidList schema_exclude_oids = {NULL, NULL};

SimpleStringList table_include_patterns = {NULL, NULL};
SimpleOidList table_include_oids = {NULL, NULL};
SimpleStringList table_exclude_patterns = {NULL, NULL};
SimpleOidList table_exclude_oids = {NULL, NULL};


/* these are to avoid passing around info for findNamespace() */
static NamespaceInfo *g_namespaces;
static int	g_numNamespaces;


/*
 * fmtQualifiedId - convert a qualified name to the proper format for
 * the source database.
 *
 * Like fmtId, use the result before calling again.
 */
const char *
fmtQualifiedId(const char *schema, const char *id)
{
	static PQExpBuffer id_return = NULL;

	if (id_return)				/* first time through? */
		resetPQExpBuffer(id_return);
	else
		id_return = createPQExpBuffer();

	/* Suppress schema name if fetching from pre-7.3 DB */
	if (schema && *schema)
	{
		appendPQExpBuffer(id_return, "%s.",
						  fmtId(schema));
	}
	appendPQExpBuffer(id_return, "%s",
					  fmtId(id));

	return id_return->data;
}


/*
 * Convenience subroutine to execute a SQL command and check for
 * COMMAND_OK status.
 */
void
do_sql_command(PGconn *conn, const char *query)
{
	PGresult   *res;

	res = PQexec(conn, query);
	check_sql_result(res, conn, query, PGRES_COMMAND_OK);
	PQclear(res);
}


/*
 * selectDumpableNamespace: policy-setting subroutine
 *		Mark a namespace as to be dumped or not
 */
static void
selectDumpableNamespace(NamespaceInfo *nsinfo)
{
	/*
	 * If specific tables are being dumped, do not dump any complete
	 * namespaces. If specific namespaces are being dumped, dump just those
	 * namespaces. Otherwise, dump all non-system namespaces.
	 */
	if (table_include_oids.head != NULL)
		nsinfo->dobj.dump = false;
	else if (schema_include_oids.head != NULL)
		nsinfo->dobj.dump = simple_oid_list_member(&schema_include_oids,
												   nsinfo->dobj.catId.oid);
	else if (strncmp(nsinfo->dobj.name, "pg_", 3) == 0 ||
			 strcmp(nsinfo->dobj.name, "information_schema") == 0 ||
			 strcmp(nsinfo->dobj.name, "gp_toolkit") == 0)
		nsinfo->dobj.dump = false;
	else
		nsinfo->dobj.dump = true;

	/*
	 * In any case, a namespace can be excluded by an exclusion switch
	 */
	if (nsinfo->dobj.dump &&
		simple_oid_list_member(&schema_exclude_oids,
							   nsinfo->dobj.catId.oid))
		nsinfo->dobj.dump = false;
}


/*
 * selectDumpableTable: policy-setting subroutine
 *		Mark a table as to be dumped or not
 */
static void
selectDumpableTable(TableInfo *tbinfo)
{
	/*
	 * If specific tables are being dumped, dump just those tables; else, dump
	 * according to the parent namespace's dump flag.
	 */
	if (table_include_oids.head != NULL)
		tbinfo->dobj.dump = simple_oid_list_member(&table_include_oids,
												   tbinfo->dobj.catId.oid);
	else
		tbinfo->dobj.dump = tbinfo->dobj.namespace->dobj.dump;

	/*
	 * In any case, a table can be excluded by an exclusion switch
	 */
	if (tbinfo->dobj.dump &&
		simple_oid_list_member(&table_exclude_oids,
							   tbinfo->dobj.catId.oid))
		tbinfo->dobj.dump = false;

	/* MPP addition */

	/*
	 * if we are dumping the mpp catalog, mark all mpp catalog tables for dump
	 * and skip the policy table since it has oids and will be populated
	 * automatically when creating the tables.
	 */
	if (g_gp_catalog_only)
	{
		if (strncmp(tbinfo->dobj.name, "gp_", 3) == 0 &&
			strcmp(tbinfo->dobj.namespace->dobj.name, "pg_catalog") == 0 &&
			strcmp(tbinfo->dobj.name, "gp_distribution_policy") != 0)
			tbinfo->dobj.dump = true;
		else
			tbinfo->dobj.dump = false;
	}
	/* MPP addition end */
}


/*
 * selectDumpableObject: policy-setting subroutine
 *		Mark a generic dumpable object as to be dumped or not
 *
 * Use this only for object types without a special-case routine above.
 */
static void
selectDumpableObject(DumpableObject *dobj)
{
	/*
	 * Default policy is to dump if parent namespace is dumpable, or always
	 * for non-namespace-associated items.
	 */
	if (dobj->namespace)
		dobj->dump = dobj->namespace->dobj.dump;
	else
		dobj->dump = true;
}


/*
 * selectDumpableType: policy-setting subroutine
 *		Mark a type as to be dumped or not
 */
static void
selectDumpableType(TypeInfo *tinfo)
{
	/* Dump only types in dumpable namespaces */
	if (!tinfo->dobj.namespace->dobj.dump)
		tinfo->dobj.dump = false;

	/* skip complex types, except for standalone composite types */
	/* (note: this test should now be unnecessary) */
	else if (OidIsValid(tinfo->typrelid) &&
			 tinfo->typrelkind != RELKIND_COMPOSITE_TYPE)
		tinfo->dobj.dump = false;

	/* skip undefined placeholder types */
	else if (!tinfo->isDefined)
		tinfo->dobj.dump = false;

	/* skip all array types that start w/ underscore */
	else if ((tinfo->dobj.name[0] == '_') &&
			 OidIsValid(tinfo->typelem))
		tinfo->dobj.dump = false;

	else
		tinfo->dobj.dump = true;
}


/*
 * findNamespace:
 *		given a namespace OID and an object OID, look up the info read by
 *		getNamespaces
 *
 * NB: for pre-7.3 source database, we use object OID to guess whether it's
 * a system object or not.	In 7.3 and later there is no guessing.
 */
static NamespaceInfo *
findNamespace(Oid nsoid, Oid objoid)
{
	int			i;

	for (i = 0; i < g_numNamespaces; i++)
	{
		NamespaceInfo *nsinfo = &g_namespaces[i];

		if (nsoid == nsinfo->dobj.catId.oid)
			return nsinfo;
	}
	mpp_err_msg(logError, progname, "schema with OID %u does not exist\n", nsoid);
	exit_nicely();

	return NULL;				/* keep compiler quiet */
}


/*
 * getDomainConstraints
 *
 * Get info about constraints on a domain.
 */
static void
getDomainConstraints(TypeInfo *tinfo)
{
	int			i;
	ConstraintInfo *constrinfo;
	PQExpBuffer query;
	PGresult   *res;
	int			i_tableoid,
				i_oid,
				i_conname,
				i_consrc;
	int			ntups;

	/*
	 * select appropriate schema to ensure names in constraint are properly
	 * qualified
	 */
	selectSourceSchema(tinfo->dobj.namespace->dobj.name);

	query = createPQExpBuffer();

	appendPQExpBuffer(query, "SELECT tableoid, oid, conname, "
					  "pg_catalog.pg_get_constraintdef(oid) AS consrc "
					  "FROM pg_catalog.pg_constraint "
					  "WHERE contypid = '%u'::pg_catalog.oid "
					  "ORDER BY conname",
					  tinfo->dobj.catId.oid);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_conname = PQfnumber(res, "conname");
	i_consrc = PQfnumber(res, "consrc");

	constrinfo = (ConstraintInfo *) malloc(ntups * sizeof(ConstraintInfo));

	tinfo->nDomChecks = ntups;
	tinfo->domChecks = constrinfo;

	for (i = 0; i < ntups; i++)
	{
		constrinfo[i].dobj.objType = DO_CONSTRAINT;
		constrinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		constrinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&constrinfo[i].dobj);
		constrinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_conname));
		constrinfo[i].dobj.namespace = tinfo->dobj.namespace;
		constrinfo[i].contable = NULL;
		constrinfo[i].condomain = tinfo;
		constrinfo[i].contype = 'c';
		constrinfo[i].condef = strdup(PQgetvalue(res, i, i_consrc));
		constrinfo[i].conindex = 0;
		constrinfo[i].coninherited = false;
		constrinfo[i].separate = false;

		/*
		 * Make the domain depend on the constraint, ensuring it won't be
		 * output till any constraint dependencies are OK.
		 */
		addObjectDependency(&tinfo->dobj,
							constrinfo[i].dobj.dumpId);
	}

	PQclear(res);

	destroyPQExpBuffer(query);
}


/*
 * Convenience subroutine to verify a SQL command succeeded,
 * and exit with a useful error message if not.
 */
void
check_sql_result(PGresult *res, PGconn *conn, const char *query,
				 ExecStatusType expected)
{
	const char *err;

	if (res && PQresultStatus(res) == expected)
		return;					/* A-OK */

	mpp_err_msg(logError, progname, "SQL command failed\n");
	if (res)
		err = PQresultErrorMessage(res);
	else
		err = PQerrorMessage(conn);
	mpp_err_msg(logError, progname, "Error message from server: %s", err);
	mpp_err_msg(logError, progname, "The command was: %s\n", query);
	exit_nicely();
}


/*
 * selectSourceSchema - make the specified schema the active search path
 * in the source database.
 *
 * NB: pg_catalog is explicitly searched after the specified schema;
 * so user names are only qualified if they are cross-schema references,
 * and system names are only qualified if they conflict with a user name
 * in the current schema.
 *
 * Whenever the selected schema is not pg_catalog, be careful to qualify
 * references to system catalogs and types in our emitted commands!
 */
void
selectSourceSchema(const char *schemaName)
{
	static char *curSchemaName = NULL;
	PQExpBuffer query;

	/* Ignore null schema names */
	if (schemaName == NULL || *schemaName == '\0')
		return;
	/* Optimize away repeated selection of same schema */
	if (curSchemaName && strcmp(curSchemaName, schemaName) == 0)
		return;

	query = createPQExpBuffer();
	appendPQExpBuffer(query, "SET search_path = %s",
					  fmtId(schemaName));
	if (strcmp(schemaName, "pg_catalog") != 0)
		appendPQExpBuffer(query, ", pg_catalog");

	do_sql_command(g_conn, query->data);

	destroyPQExpBuffer(query);
	if (curSchemaName)
		free(curSchemaName);
	curSchemaName = strdup(schemaName);
}


/*
 * Find the OIDs of all schemas matching the given list of patterns,
 * and append them to the given OID list.
 */
static void
expand_schema_name_patterns(SimpleStringList *patterns, SimpleOidList *oids)
{
	PQExpBuffer query;
	PGresult   *res;
	SimpleStringListCell *cell;
	int			i;

	if (patterns->head == NULL)
		return;					/* nothing to do */

	query = createPQExpBuffer();

	/*
	 * We use UNION ALL rather than UNION; this might sometimes result in
	 * duplicate entries in the OID list, but we don't care.
	 */

	for (cell = patterns->head; cell; cell = cell->next)
	{
		if (cell != patterns->head)
			appendPQExpBuffer(query, "UNION ALL\n");
		appendPQExpBuffer(query,
						  "SELECT oid FROM pg_catalog.pg_namespace n\n");
		processSQLNamePattern(g_conn, query, cell->val, false, false,
							  NULL, "n.nspname", NULL,
							  NULL);
	}

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	for (i = 0; i < PQntuples(res); i++)
	{
		simple_oid_list_append(oids, atooid(PQgetvalue(res, i, 0)));
	}

	PQclear(res);
	destroyPQExpBuffer(query);
}


/*
 * Find the OIDs of all tables matching the given list of patterns,
 * and append them to the given OID list.
 */
static void
expand_table_name_patterns(SimpleStringList *patterns, SimpleOidList *oids)
{
	PQExpBuffer query;
	PGresult   *res;
	SimpleStringListCell *cell;
	int			i;

	if (patterns->head == NULL)
		return;					/* nothing to do */

	query = createPQExpBuffer();

	/*
	 * We use UNION ALL rather than UNION; this might sometimes result in
	 * duplicate entries in the OID list, but we don't care.
	 */

	for (cell = patterns->head; cell; cell = cell->next)
	{
		if (cell != patterns->head)
			appendPQExpBuffer(query, "UNION ALL\n");
		appendPQExpBuffer(query,
						  "SELECT c.oid"
						  "\nFROM pg_catalog.pg_class c"
		"\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
						  "\nWHERE c.relkind in ('%c', '%c', '%c')\n",
						  RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_VIEW);
		processSQLNamePattern(g_conn, query, cell->val, true, false,
							  "n.nspname", "c.relname", NULL,
							  "pg_catalog.pg_table_is_visible(c.oid)");
	}

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	for (i = 0; i < PQntuples(res); i++)
	{
		simple_oid_list_append(oids, atooid(PQgetvalue(res, i, 0)));
	}

	PQclear(res);
	destroyPQExpBuffer(query);
}


/*
 * Scan the schema include/exclude and table include/exclude patterns
 * and develop a set of OIDs to be processed.
 */
void
convert_patterns_to_oids(void)
{
	/* Expand schema selection patterns into OID lists */
	if (schema_include_patterns.head != NULL)
	{
		expand_schema_name_patterns(&schema_include_patterns,
									&schema_include_oids);
		if (schema_include_oids.head == NULL)
		{
			mpp_err_msg(logError, progname, "No matching schemas were found\n");
			exit_nicely();
		}
	}
	expand_schema_name_patterns(&schema_exclude_patterns,
								&schema_exclude_oids);
	/* non-matching exclusion patterns aren't an error */

	/* Expand table selection patterns into OID lists */
	if (table_include_patterns.head != NULL)
	{
		expand_table_name_patterns(&table_include_patterns,
								   &table_include_oids);
		if (table_include_oids.head == NULL)
		{
			mpp_err_msg(logError, progname, "No matching tables were found\n");
			exit_nicely();
		}
	}
	expand_table_name_patterns(&table_exclude_patterns,
							   &table_exclude_oids);
	/* non-matching exclusion patterns aren't an error */
}


/*
 * getNamespaces:
 *	  read all namespaces in the system catalogs and return them in the
 * NamespaceInfo* structure
 *
 *	numNamespaces is set to the number of namespaces read in
 */
/*	Declared in pg_dump.h */
NamespaceInfo *
getNamespaces(int *numNamespaces)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query;
	NamespaceInfo *nsinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_nspname;
	int			i_rolname;
	int			i_nspacl;

	query = createPQExpBuffer();

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	/*
	 * we fetch all namespaces including system ones, so that every object we
	 * read in can be linked to a containing namespace.
	 */
	appendPQExpBuffer(query, "SELECT tableoid, oid, nspname, "
					  "(%s nspowner) as rolname, "
					  "nspacl FROM pg_namespace",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	nsinfo = (NamespaceInfo *) malloc(ntups * sizeof(NamespaceInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_nspname = PQfnumber(res, "nspname");
	i_rolname = PQfnumber(res, "rolname");
	i_nspacl = PQfnumber(res, "nspacl");

	for (i = 0; i < ntups; i++)
	{
		nsinfo[i].dobj.objType = DO_NAMESPACE;
		nsinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		nsinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&nsinfo[i].dobj);
		nsinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_nspname));
		nsinfo[i].rolname = strdup(PQgetvalue(res, i, i_rolname));
		nsinfo[i].nspacl = strdup(PQgetvalue(res, i, i_nspacl));

		/* Decide whether to dump this namespace */
		selectDumpableNamespace(&nsinfo[i]);

		if (strlen(nsinfo[i].rolname) == 0)
			mpp_err_msg(logWarn, progname, "WARNING: owner of schema \"%s\" appears to be invalid\n",
						nsinfo[i].dobj.name);
	}

	PQclear(res);
	destroyPQExpBuffer(query);

	g_namespaces = nsinfo;
	g_numNamespaces = *numNamespaces = ntups;

	return nsinfo;
}


/*
 * getFuncs:
 *	  read all the user-defined functions in the system catalogs and
 * return them in the FuncInfo* structure
 *
 * numFuncs is set to the number of functions read in
 */
/*	Declared in pg_dump.h */
FuncInfo *
getFuncs(int *numFuncs)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	FuncInfo   *finfo;
	int			i_tableoid;
	int			i_oid;
	int			i_proname;
	int			i_pronamespace;
	int			i_rolname;
	int			i_prolang;
	int			i_pronargs;
	int			i_proargtypes;
	int			i_prorettype;
	int			i_proacl;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	/* find all user-defined funcs */

	appendPQExpBuffer(query,
					  "SELECT tableoid, oid, proname, prolang, "
					  "pronargs, proargtypes, prorettype, proacl, "
					  "pronamespace, "
					  "(%s proowner) as rolname "
					  "FROM pg_proc "
					  "WHERE NOT proisagg "
					  "AND pronamespace != "
					  "(select oid from pg_namespace"
					  " where nspname = 'pg_catalog')",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	*numFuncs = ntups;

	finfo = (FuncInfo *) calloc(ntups, sizeof(FuncInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_proname = PQfnumber(res, "proname");
	i_pronamespace = PQfnumber(res, "pronamespace");
	i_rolname = PQfnumber(res, "rolname");
	i_prolang = PQfnumber(res, "prolang");
	i_pronargs = PQfnumber(res, "pronargs");
	i_proargtypes = PQfnumber(res, "proargtypes");
	i_prorettype = PQfnumber(res, "prorettype");
	i_proacl = PQfnumber(res, "proacl");

	for (i = 0; i < ntups; i++)
	{
		finfo[i].dobj.objType = DO_FUNC;
		finfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		finfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&finfo[i].dobj);
		finfo[i].dobj.name = strdup(PQgetvalue(res, i, i_proname));
		finfo[i].dobj.namespace =
			findNamespace(atooid(PQgetvalue(res, i, i_pronamespace)),
						  finfo[i].dobj.catId.oid);
		finfo[i].rolname = strdup(PQgetvalue(res, i, i_rolname));
		finfo[i].lang = atooid(PQgetvalue(res, i, i_prolang));
		finfo[i].prorettype = atooid(PQgetvalue(res, i, i_prorettype));
		finfo[i].proacl = strdup(PQgetvalue(res, i, i_proacl));
		finfo[i].nargs = atoi(PQgetvalue(res, i, i_pronargs));
		if (finfo[i].nargs == 0)
			finfo[i].argtypes = NULL;
		else
		{
			finfo[i].argtypes = (Oid *) malloc(finfo[i].nargs * sizeof(Oid));
			parseOidArray(PQgetvalue(res, i, i_proargtypes),
						  finfo[i].argtypes, finfo[i].nargs);
		}

		/* Decide whether we want to dump it */
		selectDumpableObject(&(finfo[i].dobj));

		if (strlen(finfo[i].rolname) == 0)
			mpp_err_msg(logWarn, progname,
				 "WARNING: owner of function \"%s\" appears to be invalid\n",
						finfo[i].dobj.name);
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return finfo;
}


/*
 * getTypes:
 *	  read all types in the system catalogs and return them in the
 * TypeInfo* structure
 *
 *	numTypes is set to the number of types read in
 *
 * NB: this must run after getFuncs() because we assume we can do
 * findFuncByOid().
 */
/*	Declared in pg_dump.h */
TypeInfo *
getTypes(int *numTypes)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	TypeInfo   *tinfo;
	ShellTypeInfo *stinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_typname;
	int			i_typnamespace;
	int			i_rolname;
	int			i_typinput;
	int			i_typoutput;
	int			i_typelem;
	int			i_typrelid;
	int			i_typrelkind;
	int			i_typtype;
	int			i_typisdefined;

	/*
	 * we include even the built-in types because those may be used as array
	 * elements by user-defined types
	 *
	 * we filter out the built-in types when we dump out the types
	 *
	 * same approach for undefined (shell) types
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	appendPQExpBuffer(query, "SELECT tableoid, oid, typname, "
					  "typnamespace, "
					  "(%s typowner) as rolname, "
					  "typinput::oid as typinput, "
					  "typoutput::oid as typoutput, typelem, typrelid, "
					  "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
					  "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END as typrelkind, "
					  "typtype, typisdefined "
					  "FROM pg_type",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	tinfo = (TypeInfo *) malloc(ntups * sizeof(TypeInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_typname = PQfnumber(res, "typname");
	i_typnamespace = PQfnumber(res, "typnamespace");
	i_rolname = PQfnumber(res, "rolname");
	i_typinput = PQfnumber(res, "typinput");
	i_typoutput = PQfnumber(res, "typoutput");
	i_typelem = PQfnumber(res, "typelem");
	i_typrelid = PQfnumber(res, "typrelid");
	i_typrelkind = PQfnumber(res, "typrelkind");
	i_typtype = PQfnumber(res, "typtype");
	i_typisdefined = PQfnumber(res, "typisdefined");

	for (i = 0; i < ntups; i++)
	{
		tinfo[i].dobj.objType = DO_TYPE;
		tinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		tinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&tinfo[i].dobj);
		tinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_typname));
		tinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_typnamespace)),
												tinfo[i].dobj.catId.oid);
		tinfo[i].rolname = strdup(PQgetvalue(res, i, i_rolname));
		tinfo[i].typelem = atooid(PQgetvalue(res, i, i_typelem));
		tinfo[i].typrelid = atooid(PQgetvalue(res, i, i_typrelid));
		tinfo[i].typrelkind = *PQgetvalue(res, i, i_typrelkind);
		tinfo[i].typtype = *PQgetvalue(res, i, i_typtype);
		tinfo[i].shellType = NULL;

		/*
		 * If it's a table's rowtype, use special type code to facilitate
		 * sorting into the desired order.	(We don't want to consider it an
		 * ordinary type because that would bring the table up into the
		 * datatype part of the dump order.)
		 */
		if (OidIsValid(tinfo[i].typrelid) &&
			tinfo[i].typrelkind != RELKIND_COMPOSITE_TYPE)
			tinfo[i].dobj.objType = DO_TABLE_TYPE;

		/*
		 * check for user-defined array types, omit system generated ones
		 */
		if (OidIsValid(tinfo[i].typelem) &&
			tinfo[i].dobj.name[0] != '_')
			tinfo[i].isArray = true;
		else
			tinfo[i].isArray = false;

		if (strcmp(PQgetvalue(res, i, i_typisdefined), "t") == 0)
			tinfo[i].isDefined = true;
		else
			tinfo[i].isDefined = false;

		/* Decide whether we want to dump it */
		selectDumpableType(&tinfo[i]);

		/*
		 * If it's a domain, fetch info about its constraints, if any
		 */
		tinfo[i].nDomChecks = 0;
		tinfo[i].domChecks = NULL;
		if (tinfo[i].dobj.dump && tinfo[i].typtype == 'd')
			getDomainConstraints(&(tinfo[i]));

		/*
		 * If it's a base type, make a DumpableObject representing a shell
		 * definition of the type.	We will need to dump that ahead of the I/O
		 * functions for the type.
		 *
		 * Note: the shell type doesn't have a catId.  You might think it
		 * should copy the base type's catId, but then it might capture the
		 * pg_depend entries for the type, which we don't want.
		 */
		if (tinfo[i].dobj.dump && tinfo[i].typtype == 'b')
		{
			stinfo = (ShellTypeInfo *) malloc(sizeof(ShellTypeInfo));
			stinfo->dobj.objType = DO_SHELL_TYPE;
			stinfo->dobj.catId = nilCatalogId;
			AssignDumpId(&stinfo->dobj);
			stinfo->dobj.name = strdup(tinfo[i].dobj.name);
			stinfo->dobj.namespace = tinfo[i].dobj.namespace;
			stinfo->baseType = &(tinfo[i]);
			tinfo[i].shellType = stinfo;

			/*
			 * Initially mark the shell type as not to be dumped.  We'll only
			 * dump it if the I/O functions need to be dumped; this is taken
			 * care of while sorting dependencies.
			 */
			stinfo->dobj.dump = false;
		}

		if (strlen(tinfo[i].rolname) == 0 && tinfo[i].isDefined)
			mpp_err_msg(logWarn, progname, "WARNING: owner of data type \"%s\" appears to be invalid\n",
						tinfo[i].dobj.name);
	}

	*numTypes = ntups;

	PQclear(res);

	destroyPQExpBuffer(query);

	return tinfo;
}


/*
 * getTypeStorageOptions:
 *	  read all types with storage options in the system catalogs and return them in the
 * TypeStorageOptions* structure
 *
 *	numTypes is set to the number of types with storage options read in
 *
 */
TypeStorageOptions *
getTypeStorageOptions(int *numTypes)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	TypeStorageOptions   *tstorageoptions;
	int			i_oid;
	int			i_typname;
	int			i_typnamespace;
	int			i_typoptions;
	int			i_rolname;

	if (g_gp_supportsAttributeEncoding == false)
	{
		numTypes = 0;
		tstorageoptions = (TypeStorageOptions *) malloc(0);
		return tstorageoptions;
	}

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	/*
	 * The following statement used format_type to resolve an internal name to its equivalent sql name.
	 * The format_type seems to do two things, it translates an internal type name (e.g. bpchar) into its
	 * sql equivalent (e.g. character), and it puts trailing "[]" on a type if it is an array.
	 * For any user defined type (ie. oid > 10000) or any type that might be an array (ie. starts with '_'),
	 * then we will call quote_ident. If the type is a system defined type (i.e. oid <= 10000)
	 * and can not possibly be an array (i.e. does not start with '_'), then call format_type to get the name. The
	 * reason we do not call format_type for arrays is that it will return a '[]' on the end, which can not be used
	 * when dumping the type.
	 */
	appendPQExpBuffer(query, "SELECT "
      " CASE WHEN t.oid > 10000 OR substring(t.typname from 1 for 1) = '_' "
      " THEN  quote_ident(t.typname) "
      " ELSE  pg_catalog.format_type(t.oid, NULL) "
      " END   as typname "
			", t.oid AS oid"
			", t.typnamespace AS typnamespace"
			", (%s typowner) as rolname"
			", array_to_string(a.typoptions, ', ') AS typoptions "
			" FROM pg_type AS t "
			" INNER JOIN pg_catalog.pg_type_encoding a ON a.typid = t.oid"
			" WHERE t.typisdefined = 't'", username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	tstorageoptions = (TypeStorageOptions *) malloc(ntups * sizeof(TypeStorageOptions));

	i_typname = PQfnumber(res, "typname");
	i_oid = PQfnumber(res, "oid");
	i_typnamespace = PQfnumber(res, "typnamespace");
	i_typoptions = PQfnumber(res, "typoptions");
	i_rolname = PQfnumber(res, "rolname");

	for (i = 0; i < ntups; i++)
	{
		tstorageoptions[i].dobj.objType = DO_TYPE_STORAGE_OPTIONS;
		AssignDumpId(&tstorageoptions[i].dobj);
		tstorageoptions[i].dobj.name = strdup(PQgetvalue(res, i, i_typname));
		tstorageoptions[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		tstorageoptions[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_typnamespace)),tstorageoptions[i].dobj.catId.oid);
		tstorageoptions[i].typoptions = strdup(PQgetvalue(res, i, i_typoptions));
		tstorageoptions[i].rolname = strdup(PQgetvalue(res, i, i_rolname));
	}

	*numTypes = ntups;

	PQclear(res);

	destroyPQExpBuffer(query);

	return tstorageoptions;
}




/*
 * getOperators:
 *	  read all operators in the system catalogs and return them in the
 * OprInfo* structure
 *
 *	numOprs is set to the number of operators read in
 */
/*	Declared in pg_dump.h */
OprInfo *
getOperators(int *numOprs)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	OprInfo    *oprinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_oprname;
	int			i_oprnamespace;
	int			i_rolname;
	int			i_oprcode;

	/*
	 * find all operators, including builtin operators; we filter out
	 * system-defined operators at dump-out time.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	appendPQExpBuffer(query, "SELECT tableoid, oid, oprname, "
					  "oprnamespace, "
					  "(%s oprowner) as rolname, "
					  "oprcode::oid as oprcode "
					  "FROM pg_operator",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);
	*numOprs = ntups;

	oprinfo = (OprInfo *) malloc(ntups * sizeof(OprInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_oprname = PQfnumber(res, "oprname");
	i_oprnamespace = PQfnumber(res, "oprnamespace");
	i_rolname = PQfnumber(res, "rolname");
	i_oprcode = PQfnumber(res, "oprcode");

	for (i = 0; i < ntups; i++)
	{
		oprinfo[i].dobj.objType = DO_OPERATOR;
		oprinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		oprinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&oprinfo[i].dobj);
		oprinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_oprname));
		oprinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_oprnamespace)),
												  oprinfo[i].dobj.catId.oid);
		oprinfo[i].rolname = strdup(PQgetvalue(res, i, i_rolname));
		oprinfo[i].oprcode = atooid(PQgetvalue(res, i, i_oprcode));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(oprinfo[i].dobj));

		if (strlen(oprinfo[i].rolname) == 0)
			mpp_err_msg(logWarn, progname, "WARNING: owner of operator \"%s\" appears to be invalid\n",
						oprinfo[i].dobj.name);
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return oprinfo;
}


/*
 * getConversions:
 *	  read all conversions in the system catalogs and return them in the
 * ConvInfo* structure
 *
 *	numConversions is set to the number of conversions read in
 */
/*	Declared in pg_dump.h */
ConvInfo *
getConversions(int *numConversions)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	ConvInfo   *convinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_conname;
	int			i_connamespace;
	int			i_rolname;

	/*
	 * find all conversions, including builtin conversions; we filter out
	 * system-defined conversions at dump-out time.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	appendPQExpBuffer(query, "SELECT tableoid, oid, conname, "
					  "connamespace, "
					  "(%s conowner) as rolname "
					  "FROM pg_conversion",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);
	*numConversions = ntups;

	convinfo = (ConvInfo *) malloc(ntups * sizeof(ConvInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_conname = PQfnumber(res, "conname");
	i_connamespace = PQfnumber(res, "connamespace");
	i_rolname = PQfnumber(res, "rolname");

	for (i = 0; i < ntups; i++)
	{
		convinfo[i].dobj.objType = DO_CONVERSION;
		convinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		convinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&convinfo[i].dobj);
		convinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_conname));
		convinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_connamespace)),
												 convinfo[i].dobj.catId.oid);
		convinfo[i].rolname = strdup(PQgetvalue(res, i, i_rolname));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(convinfo[i].dobj));
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return convinfo;
}


/*
 * getOpclasses:
 *	  read all opclasses in the system catalogs and return them in the
 * OpclassInfo* structure
 *
 *	numOpclasses is set to the number of opclasses read in
 */
/*	Declared in pg_dump.h */
OpclassInfo *
getOpclasses(int *numOpclasses)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	OpclassInfo *opcinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_opcname;
	int			i_opcnamespace;
	int			i_rolname;

	/*
	 * find all opclasses, including builtin opclasses; we filter out
	 * system-defined opclasses at dump-out time.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	appendPQExpBuffer(query, "SELECT tableoid, oid, opcname, "
					  "opcnamespace, "
					  "(%s opcowner) as rolname "
					  "FROM pg_opclass",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);
	*numOpclasses = ntups;

	opcinfo = (OpclassInfo *) malloc(ntups * sizeof(OpclassInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_opcname = PQfnumber(res, "opcname");
	i_opcnamespace = PQfnumber(res, "opcnamespace");
	i_rolname = PQfnumber(res, "rolname");

	for (i = 0; i < ntups; i++)
	{
		opcinfo[i].dobj.objType = DO_OPCLASS;
		opcinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		opcinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&opcinfo[i].dobj);
		opcinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_opcname));
		opcinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_opcnamespace)),
												  opcinfo[i].dobj.catId.oid);
		opcinfo[i].rolname = strdup(PQgetvalue(res, i, i_rolname));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(opcinfo[i].dobj));

		if (strlen(opcinfo[i].rolname) == 0)
			mpp_err_msg(logWarn, progname, "WARNING: owner of operator class \"%s\" appears to be invalid\n",
						opcinfo[i].dobj.name);
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return opcinfo;
}


/*
 * getAggregates:
 *	  read all the user-defined aggregates in the system catalogs and
 * return them in the AggInfo* structure
 *
 * numAggs is set to the number of aggregates read in
 */
/*	Declared in pg_dump.h */
AggInfo *
getAggregates(int *numAggs)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	AggInfo    *agginfo;
	int			i_tableoid;
	int			i_oid;
	int			i_aggname;
	int			i_aggnamespace;
	int			i_pronargs;
	int			i_proargtypes;
	int			i_rolname;
	int			i_aggacl;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	/* find all user-defined aggregates */

	appendPQExpBuffer(query, "SELECT tableoid, oid, proname as aggname, "
					  "pronamespace as aggnamespace, "
					  "pronargs, proargtypes, "
					  "(%s proowner) as rolname, "
					  "proacl as aggacl "
					  "FROM pg_proc "
					  "WHERE proisagg "
					  "AND pronamespace != "
			   "(select oid from pg_namespace where nspname = 'pg_catalog')",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);
	*numAggs = ntups;

	agginfo = (AggInfo *) malloc(ntups * sizeof(AggInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_aggname = PQfnumber(res, "aggname");
	i_aggnamespace = PQfnumber(res, "aggnamespace");
	i_pronargs = PQfnumber(res, "pronargs");
	i_proargtypes = PQfnumber(res, "proargtypes");
	i_rolname = PQfnumber(res, "rolname");
	i_aggacl = PQfnumber(res, "aggacl");

	for (i = 0; i < ntups; i++)
	{
		agginfo[i].aggfn.dobj.objType = DO_AGG;
		agginfo[i].aggfn.dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		agginfo[i].aggfn.dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&agginfo[i].aggfn.dobj);
		agginfo[i].aggfn.dobj.name = strdup(PQgetvalue(res, i, i_aggname));
		agginfo[i].aggfn.dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_aggnamespace)),
											agginfo[i].aggfn.dobj.catId.oid);
		agginfo[i].aggfn.rolname = strdup(PQgetvalue(res, i, i_rolname));
		if (strlen(agginfo[i].aggfn.rolname) == 0)
			mpp_err_msg(logWarn, progname, "WARNING: owner of aggregate function \"%s\" appears to be invalid\n",
						agginfo[i].aggfn.dobj.name);
		agginfo[i].aggfn.lang = InvalidOid;		/* not currently interesting */
		agginfo[i].aggfn.prorettype = InvalidOid;		/* not saved */
		agginfo[i].aggfn.proacl = strdup(PQgetvalue(res, i, i_aggacl));
		agginfo[i].aggfn.nargs = atoi(PQgetvalue(res, i, i_pronargs));
		if (agginfo[i].aggfn.nargs == 0)
			agginfo[i].aggfn.argtypes = NULL;
		else
		{
			agginfo[i].aggfn.argtypes = (Oid *) malloc(agginfo[i].aggfn.nargs * sizeof(Oid));
			parseOidArray(PQgetvalue(res, i, i_proargtypes),
						  agginfo[i].aggfn.argtypes,
						  agginfo[i].aggfn.nargs);
		}

		/* Decide whether we want to dump it */
		selectDumpableObject(&(agginfo[i].aggfn.dobj));
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return agginfo;
}

/*
 * getExtProtocols:
 *	  read all the user-defined protocols in the system catalogs and
 * return them in the ExtProtInfo* structure
 *
 * numExtProtocols is set to the number of protocols read in
 */
/*	Declared in pg_dump.h */
ExtProtInfo *
getExtProtocols(int *numExtProtocols)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	ExtProtInfo *ptcinfo;
	int			i_oid;
	int			i_tableoid;
	int			i_ptcname;
	int			i_rolname;
	int			i_ptcacl;
	int			i_ptctrusted;
	int 		i_ptcreadid;
	int			i_ptcwriteid;
	int			i_ptcvalidid;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	/* find all user-defined aggregates */

	appendPQExpBuffer(query, "SELECT ptc.tableoid as tableoid, "
							 "       ptc.oid as oid, "
							 "       ptc.ptcname as ptcname, "
							 "       ptcreadfn as ptcreadoid, "
							 "       ptcwritefn as ptcwriteoid, "
							 "		 ptcvalidatorfn as ptcvaloid, "
							 "       (%s ptc.ptcowner) as rolname, "
							 "       ptc.ptctrusted as ptctrusted, "
							 "       ptc.ptcacl as ptcacl "
							 "FROM   pg_extprotocol ptc",
							 	 	 username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);
	*numExtProtocols = ntups;

	ptcinfo = (ExtProtInfo *) malloc(ntups * sizeof(ExtProtInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_ptcname = PQfnumber(res, "ptcname");
	i_rolname = PQfnumber(res, "rolname");
	i_ptcacl = PQfnumber(res, "ptcacl");
	i_ptctrusted = PQfnumber(res, "ptctrusted");
	i_ptcreadid = PQfnumber(res, "ptcreadoid");
	i_ptcwriteid = PQfnumber(res, "ptcwriteoid");
	i_ptcvalidid = PQfnumber(res, "ptcvaloid");

	for (i = 0; i < ntups; i++)
	{
		ptcinfo[i].dobj.objType = DO_EXTPROTOCOL;
		ptcinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		ptcinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&ptcinfo[i].dobj);
		ptcinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_ptcname));
		ptcinfo[i].dobj.namespace = NULL;
		ptcinfo[i].ptcowner = strdup(PQgetvalue(res, i, i_rolname));
		if (strlen(ptcinfo[i].ptcowner) == 0)
			mpp_err_msg(logWarn, progname, "WARNING: owner of external protocol \"%s\" appears to be invalid\n",
						ptcinfo[i].dobj.name);


		if (PQgetisnull(res, i, i_ptcreadid))
			ptcinfo[i].ptcreadid = InvalidOid;
		else
			ptcinfo[i].ptcreadid = atooid(PQgetvalue(res, i, i_ptcreadid));

		if (PQgetisnull(res, i, i_ptcwriteid))
			ptcinfo[i].ptcwriteid = InvalidOid;
		else
			ptcinfo[i].ptcwriteid = atooid(PQgetvalue(res, i, i_ptcwriteid));

		if (PQgetisnull(res, i, i_ptcvalidid))
			ptcinfo[i].ptcvalidid = InvalidOid;
		else
			ptcinfo[i].ptcvalidid = atooid(PQgetvalue(res, i, i_ptcvalidid));

		ptcinfo[i].ptcacl = strdup(PQgetvalue(res, i, i_ptcacl));
		ptcinfo[i].ptctrusted = *(PQgetvalue(res, i, i_ptctrusted)) == 't';

		/* Decide whether we want to dump it */
		selectDumpableObject(&(ptcinfo[i].dobj));
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return ptcinfo;
}

/*
 * getForeignDataWrappers:
 *	  read all foreign-data wrappers in the system catalogs and return
 *	  them in the FdwInfo* structure
 *
 *	numForeignDataWrappers is set to the number of fdws read in
 */
FdwInfo *
getForeignDataWrappers(int *numForeignDataWrappers)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	FdwInfo    *fdwinfo;
	int			i_oid;
	int			i_fdwname;
	int			i_rolname;
	int			i_fdwvalidator;
	int			i_fdwacl;
	int			i_fdwoptions;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	appendPQExpBuffer(query, "SELECT oid, fdwname, "
		"(%s fdwowner) AS rolname, fdwvalidator::pg_catalog.regproc, fdwacl,"
					  "array_to_string(ARRAY("
		 "		SELECT option_name || ' ' || quote_literal(option_value) "
	   "		FROM pg_options_to_table(fdwoptions)), ', ') AS fdwoptions "
					  "FROM pg_foreign_data_wrapper",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);
	*numForeignDataWrappers = ntups;

	fdwinfo = (FdwInfo *) malloc(ntups * sizeof(FdwInfo));

	i_oid = PQfnumber(res, "oid");
	i_fdwname = PQfnumber(res, "fdwname");
	i_rolname = PQfnumber(res, "rolname");
	i_fdwvalidator = PQfnumber(res, "fdwvalidator");
	i_fdwacl = PQfnumber(res, "fdwacl");
	i_fdwoptions = PQfnumber(res, "fdwoptions");

	for (i = 0; i < ntups; i++)
	{
		fdwinfo[i].dobj.objType = DO_FDW;
		fdwinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&fdwinfo[i].dobj);
		fdwinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_fdwname));
		fdwinfo[i].dobj.namespace = NULL;
		fdwinfo[i].rolname = strdup(PQgetvalue(res, i, i_rolname));
		fdwinfo[i].fdwvalidator = strdup(PQgetvalue(res, i, i_fdwvalidator));
		fdwinfo[i].fdwoptions = strdup(PQgetvalue(res, i, i_fdwoptions));
		fdwinfo[i].fdwacl = strdup(PQgetvalue(res, i, i_fdwacl));


		/* Decide whether we want to dump it */
		selectDumpableObject(&(fdwinfo[i].dobj));
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return fdwinfo;
}

/*
 * getForeignServers:
 *	  read all foreign servers in the system catalogs and return
 *	  them in the ForeignServerInfo * structure
 *
 *	numForeignServers is set to the number of servers read in
 */
ForeignServerInfo *
getForeignServers(int *numForeignServers)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	ForeignServerInfo *srvinfo;
	int			i_oid;
	int			i_srvname;
	int			i_rolname;
	int			i_srvfdw;
	int			i_srvtype;
	int			i_srvversion;
	int			i_srvacl;
	int			i_srvoptions;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	appendPQExpBuffer(query, "SELECT oid, srvname, "
					  "(%s srvowner) AS rolname, "
					  "srvfdw, srvtype, srvversion, srvacl,"
					  "array_to_string(ARRAY("
		 "		SELECT option_name || ' ' || quote_literal(option_value) "
	   "		FROM pg_options_to_table(srvoptions)), ', ') AS srvoptions "
					  "FROM pg_foreign_server",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);
	*numForeignServers = ntups;

	srvinfo = (ForeignServerInfo *) malloc(ntups * sizeof(ForeignServerInfo));

	i_oid = PQfnumber(res, "oid");
	i_srvname = PQfnumber(res, "srvname");
	i_rolname = PQfnumber(res, "rolname");
	i_srvfdw = PQfnumber(res, "srvfdw");
	i_srvtype = PQfnumber(res, "srvtype");
	i_srvversion = PQfnumber(res, "srvversion");
	i_srvacl = PQfnumber(res, "srvacl");
	i_srvoptions = PQfnumber(res, "srvoptions");

	for (i = 0; i < ntups; i++)
	{
		srvinfo[i].dobj.objType = DO_FOREIGN_SERVER;
		srvinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&srvinfo[i].dobj);
		srvinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_srvname));
		srvinfo[i].dobj.namespace = NULL;
		srvinfo[i].rolname = strdup(PQgetvalue(res, i, i_rolname));
		srvinfo[i].srvfdw = atooid(PQgetvalue(res, i, i_srvfdw));
		srvinfo[i].srvtype = strdup(PQgetvalue(res, i, i_srvtype));
		srvinfo[i].srvversion = strdup(PQgetvalue(res, i, i_srvversion));
		srvinfo[i].srvoptions = strdup(PQgetvalue(res, i, i_srvoptions));
		srvinfo[i].srvacl = strdup(PQgetvalue(res, i, i_srvacl));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(srvinfo[i].dobj));
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return srvinfo;
}

/*
 * getTables
 *	  read all the user-defined tables (no indexes, no catalogs)
 * in the system catalogs return them in the TableInfo* structure
 *
 * numTables is set to the number of tables read in
 */
/*	Declared in pg_dump.h */
TableInfo *
getTables(int *numTables)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	PQExpBuffer delqry = createPQExpBuffer();
	PQExpBuffer lockquery = createPQExpBuffer();
	TableInfo  *tblinfo;
	int			i_reltableoid;
	int			i_reloid;
	int			i_relname;
	int			i_relnamespace;
	int			i_relkind;
	int			i_relstorage;
	int			i_relacl;
	int			i_rolname;
	int			i_relchecks;
	int			i_reltriggers;
	int			i_relhasindex;
	int			i_relhasrules;
	int			i_relhasoids;
	int			i_owning_tab;
	int			i_owning_col;
	int			i_reltablespace;
	int			i_reloptions;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	/*
	 * Find all the tables (including views and sequences).
	 *
	 * We include system catalogs, so that we can work if a user table is
	 * defined to inherit from a system catalog (pretty weird, but...)
	 *
	 * We ignore tables that are not type 'r' (ordinary relation), 'S'
	 * (sequence), 'v' (view), or 'c' (composite type).
	 *
	 * Composite-type table entries won't be dumped as such, but we have to
	 * make a DumpableObject for them so that we can track dependencies of the
	 * composite type (pg_depend entries for columns of the composite type
	 * link to the pg_class entry not the pg_type entry).
	 *
	 * Note: in this phase we should collect only a minimal amount of
	 * information about each table, basically just enough to decide if it is
	 * interesting. We must fetch all tables in this phase because otherwise
	 * we cannot correctly identify inherited columns, owned sequences, etc.
	 */

	/*
	 * Left join to pick up dependency info linking sequences to their owning
	 * column, if any (note this dependency is AUTO as of 8.2)
	 */
	appendPQExpBuffer(query,
					  "SELECT c.tableoid, c.oid, relname, "
					  "relacl, relkind, relstorage, relnamespace, "
					  "(%s relowner) as rolname, "
					  "relchecks, reltriggers, "
					  "relhasindex, relhasrules, relhasoids, "
					  "d.refobjid as owning_tab, "
					  "d.refobjsubid as owning_col, "
					  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
					  "array_to_string(c.reloptions, ', ') as reloptions "
					  "from pg_class c "
					  "left join pg_depend d on "
					  "(c.relkind = '%c' and "
					  "d.classid = c.tableoid and d.objid = c.oid and "
					  "d.objsubid = 0 and "
					  "d.refclassid = c.tableoid and d.deptype = 'a') "
					  "where relkind in ('%c', '%c', '%c', '%c') %s"
					  "order by c.oid",
					  username_subquery,
					  RELKIND_SEQUENCE,
					  RELKIND_RELATION, RELKIND_SEQUENCE,
					  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE,
					  g_gp_supportsPartitioning ?
					  "AND c.oid NOT IN (select parchildrelid from "
					  "pg_partition_rule)" : "");

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	*numTables = ntups;

	/*
	 * Extract data from result and lock dumpable tables.  We do the locking
	 * before anything else, to minimize the window wherein a table could
	 * disappear under us.
	 *
	 * Note that we have to save info about all tables here, even when dumping
	 * only one, because we don't yet know which tables might be inheritance
	 * ancestors of the target table.
	 */
	tblinfo = (TableInfo *) calloc(ntups, sizeof(TableInfo));

	i_reltableoid = PQfnumber(res, "tableoid");
	i_reloid = PQfnumber(res, "oid");
	i_relname = PQfnumber(res, "relname");
	i_relnamespace = PQfnumber(res, "relnamespace");
	i_relacl = PQfnumber(res, "relacl");
	i_relkind = PQfnumber(res, "relkind");
	i_relstorage = PQfnumber(res, "relstorage");
	i_rolname = PQfnumber(res, "rolname");
	i_relchecks = PQfnumber(res, "relchecks");
	i_reltriggers = PQfnumber(res, "reltriggers");
	i_relhasindex = PQfnumber(res, "relhasindex");
	i_relhasrules = PQfnumber(res, "relhasrules");
	i_relhasoids = PQfnumber(res, "relhasoids");
	i_owning_tab = PQfnumber(res, "owning_tab");
	i_owning_col = PQfnumber(res, "owning_col");
	i_reltablespace = PQfnumber(res, "reltablespace");
	i_reloptions = PQfnumber(res, "reloptions");

	for (i = 0; i < ntups; i++)
	{
		tblinfo[i].dobj.objType = DO_TABLE;
		tblinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_reltableoid));
		tblinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_reloid));
		AssignDumpId(&tblinfo[i].dobj);
		tblinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_relname));
		tblinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_relnamespace)),
												  tblinfo[i].dobj.catId.oid);
		tblinfo[i].rolname = strdup(PQgetvalue(res, i, i_rolname));
		tblinfo[i].relacl = strdup(PQgetvalue(res, i, i_relacl));
		tblinfo[i].relkind = *(PQgetvalue(res, i, i_relkind));
		tblinfo[i].relstorage = *(PQgetvalue(res, i, i_relstorage));
		tblinfo[i].hasindex = (strcmp(PQgetvalue(res, i, i_relhasindex), "t") == 0);
		tblinfo[i].hasrules = (strcmp(PQgetvalue(res, i, i_relhasrules), "t") == 0);
		tblinfo[i].hasoids = (strcmp(PQgetvalue(res, i, i_relhasoids), "t") == 0);
		tblinfo[i].ncheck = atoi(PQgetvalue(res, i, i_relchecks));
		tblinfo[i].ntrig = atoi(PQgetvalue(res, i, i_reltriggers));
		if (PQgetisnull(res, i, i_owning_tab))
		{
			tblinfo[i].owning_tab = InvalidOid;
			tblinfo[i].owning_col = 0;
		}
		else
		{
			tblinfo[i].owning_tab = atooid(PQgetvalue(res, i, i_owning_tab));
			tblinfo[i].owning_col = atoi(PQgetvalue(res, i, i_owning_col));
		}
		tblinfo[i].reltablespace = strdup(PQgetvalue(res, i, i_reltablespace));
		tblinfo[i].reloptions = strdup(PQgetvalue(res, i, i_reloptions));

		/* other fields were zeroed above */

		/*
		 * Decide whether we want to dump this table.
		 */
		if (tblinfo[i].relkind == RELKIND_COMPOSITE_TYPE)
			tblinfo[i].dobj.dump = false;
		else
			selectDumpableTable(&tblinfo[i]);
		tblinfo[i].interesting = tblinfo[i].dobj.dump;

		/*
		 * Lock target tables to make sure they aren't DROPPED or altered in
		 * schema before we get around to dumping them.
		 *
		 * Note that we don't explicitly lock parents of the target tables; we
		 * assume our lock on the child is enough to prevent schema
		 * alterations to parent tables.
		 *
		 * NOTE: it'd be kinda nice to lock views and sequences too, not only
		 * plain tables, but the backend doesn't presently allow that.
		 */
		if (tblinfo[i].dobj.dump && tblinfo[i].relkind == RELKIND_RELATION)
		{
			resetPQExpBuffer(lockquery);
			appendPQExpBuffer(lockquery, "LOCK TABLE %s IN %s MODE",
						 fmtQualifiedId(tblinfo[i].dobj.namespace->dobj.name,
									 tblinfo[i].dobj.name), table_lock_mode);
			do_sql_command(g_conn, lockquery->data);
		}

		/* Emit notice if join for owner failed */
		if (strlen(tblinfo[i].rolname) == 0)
			mpp_err_msg(logWarn, progname, "WARNING: owner of table \"%s\" appears to be invalid\n",
						tblinfo[i].dobj.name);
	}

	PQclear(res);

	/*
	 * Force sequences that are "owned" by table columns to be dumped whenever
	 * their owning table is being dumped.
	 */
	for (i = 0; i < ntups; i++)
	{
		TableInfo  *seqinfo = &tblinfo[i];
		int			j;

		if (!OidIsValid(seqinfo->owning_tab))
			continue;			/* not an owned sequence */
		if (seqinfo->dobj.dump)
			continue;			/* no need to search */

		/* can't use findTableByOid yet, unfortunately */
		for (j = 0; j < ntups; j++)
		{
			if (tblinfo[j].dobj.catId.oid == seqinfo->owning_tab)
			{
				if (tblinfo[j].dobj.dump)
				{
					seqinfo->interesting = true;
					seqinfo->dobj.dump = true;
				}
				break;
			}
		}
	}

	destroyPQExpBuffer(query);
	destroyPQExpBuffer(delqry);
	destroyPQExpBuffer(lockquery);

	return tblinfo;
}


/*
 * getInherits
 *	  read all the inheritance information
 * from the system catalogs return them in the InhInfo* structure
 *
 * numInherits is set to the number of pairs read in
 */
/*	Declared in pg_dump.h */
InhInfo *
getInherits(int *numInherits)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	InhInfo    *inhinfo;

	int			i_inhrelid;
	int			i_inhparent;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	/* find all the inheritance information */

	appendPQExpBuffer(query, "SELECT inhrelid, inhparent from pg_inherits");

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	*numInherits = ntups;

	inhinfo = (InhInfo *) malloc(ntups * sizeof(InhInfo));

	i_inhrelid = PQfnumber(res, "inhrelid");
	i_inhparent = PQfnumber(res, "inhparent");

	for (i = 0; i < ntups; i++)
	{
		inhinfo[i].inhrelid = atooid(PQgetvalue(res, i, i_inhrelid));
		inhinfo[i].inhparent = atooid(PQgetvalue(res, i, i_inhparent));
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return inhinfo;
}


/*
 * getIndexes
 *	  get information about every index on a dumpable table
 *
 * Note: index data is not returned directly to the caller, but it
 * does get entered into the DumpableObject tables.
 */
/*	Declared in pg_dump.h */
void
getIndexes(TableInfo tblinfo[], int numTables)
{
	int			i,
				j;
	PQExpBuffer query = createPQExpBuffer();
	PGresult   *res;
	IndxInfo   *indxinfo;
	ConstraintInfo *constrinfo;
	int			i_tableoid,
				i_oid,
				i_indexname,
				i_indexdef,
				i_indnkeys,
				i_indkey,
				i_indisclustered,
				i_contype,
				i_conname,
				i_contableoid,
				i_conoid,
				i_tablespace,
				i_options;
	int			ntups;

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		/* Only plain tables have indexes */
		if (tbinfo->relkind != RELKIND_RELATION || !tbinfo->hasindex)
			continue;

		/* Ignore indexes of tables not to be dumped */
		if (!tbinfo->dobj.dump)
			continue;

		if (g_verbose)
			mpp_err_msg(logInfo, progname, "reading indexes for table \"%s\"\n",
						tbinfo->dobj.name);

		/* Make sure we are in proper schema so indexdef is right */
		selectSourceSchema(tbinfo->dobj.namespace->dobj.name);

		/*
		 * The point of the messy-looking outer join is to find a constraint
		 * that is related by an internal dependency link to the index. If we
		 * find one, create a CONSTRAINT entry linked to the INDEX entry.  We
		 * assume an index won't have more than one internal dependency.
		 */
		resetPQExpBuffer(query);
		appendPQExpBuffer(query,
						  "SELECT t.tableoid, t.oid, "
						  "t.relname as indexname, "
					 "pg_catalog.pg_get_indexdef(i.indexrelid) as indexdef, "
						  "t.relnatts as indnkeys, "
						  "i.indkey, i.indisclustered, "
						  "c.contype, c.conname, "
						  "c.tableoid as contableoid, "
						  "c.oid as conoid, "
						  "(SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) as tablespace, "
						  "array_to_string(t.reloptions, ', ') as options "
						  "FROM pg_catalog.pg_index i "
					  "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
						  "LEFT JOIN pg_catalog.pg_depend d "
						  "ON (d.classid = t.tableoid "
						  "AND d.objid = t.oid "
						  "AND d.deptype = 'i') "
						  "LEFT JOIN pg_catalog.pg_constraint c "
						  "ON (d.refclassid = c.tableoid "
						  "AND d.refobjid = c.oid) "
						  "WHERE i.indrelid = '%u'::pg_catalog.oid "
						  "ORDER BY indexname",
						  tbinfo->dobj.catId.oid);

		res = PQexec(g_conn, query->data);
		check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

		ntups = PQntuples(res);

		i_tableoid = PQfnumber(res, "tableoid");
		i_oid = PQfnumber(res, "oid");
		i_indexname = PQfnumber(res, "indexname");
		i_indexdef = PQfnumber(res, "indexdef");
		i_indnkeys = PQfnumber(res, "indnkeys");
		i_indkey = PQfnumber(res, "indkey");
		i_indisclustered = PQfnumber(res, "indisclustered");
		i_contype = PQfnumber(res, "contype");
		i_conname = PQfnumber(res, "conname");
		i_contableoid = PQfnumber(res, "contableoid");
		i_conoid = PQfnumber(res, "conoid");
		i_tablespace = PQfnumber(res, "tablespace");
		i_options = PQfnumber(res, "options");

		indxinfo = (IndxInfo *) malloc(ntups * sizeof(IndxInfo));
		constrinfo = (ConstraintInfo *) malloc(ntups * sizeof(ConstraintInfo));

		for (j = 0; j < ntups; j++)
		{
			char		contype;

			indxinfo[j].dobj.objType = DO_INDEX;
			indxinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_tableoid));
			indxinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_oid));
			AssignDumpId(&indxinfo[j].dobj);
			indxinfo[j].dobj.name = strdup(PQgetvalue(res, j, i_indexname));
			indxinfo[j].dobj.namespace = tbinfo->dobj.namespace;
			indxinfo[j].indextable = tbinfo;
			indxinfo[j].indexdef = strdup(PQgetvalue(res, j, i_indexdef));
			indxinfo[j].indnkeys = atoi(PQgetvalue(res, j, i_indnkeys));
			indxinfo[j].tablespace = strdup(PQgetvalue(res, j, i_tablespace));
			indxinfo[j].options = strdup(PQgetvalue(res, j, i_options));

			/*
			 * In pre-7.4 releases, indkeys may contain more entries than
			 * indnkeys says (since indnkeys will be 1 for a functional
			 * index). We don't actually care about this case since we don't
			 * examine indkeys except for indexes associated with PRIMARY and
			 * UNIQUE constraints, which are never functional indexes. But we
			 * have to allocate enough space to keep parseOidArray from
			 * complaining.
			 */
			indxinfo[j].indkeys = (Oid *) malloc(INDEX_MAX_KEYS * sizeof(Oid));
			parseOidArray(PQgetvalue(res, j, i_indkey),
						  indxinfo[j].indkeys, INDEX_MAX_KEYS);
			indxinfo[j].indisclustered = (PQgetvalue(res, j, i_indisclustered)[0] == 't');
			contype = *(PQgetvalue(res, j, i_contype));

			if (contype == 'p' || contype == 'u')
			{
				/*
				 * If we found a constraint matching the index, create an
				 * entry for it.
				 *
				 * In a pre-7.3 database, we take this path iff the index was
				 * marked indisprimary.
				 */
				constrinfo[j].dobj.objType = DO_CONSTRAINT;
				constrinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_contableoid));
				constrinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_conoid));
				AssignDumpId(&constrinfo[j].dobj);
				constrinfo[j].dobj.name = strdup(PQgetvalue(res, j, i_conname));
				constrinfo[j].dobj.namespace = tbinfo->dobj.namespace;
				constrinfo[j].contable = tbinfo;
				constrinfo[j].condomain = NULL;
				constrinfo[j].contype = contype;
				constrinfo[j].condef = NULL;
				constrinfo[j].conindex = indxinfo[j].dobj.dumpId;
				constrinfo[j].coninherited = false;
				constrinfo[j].separate = true;

				indxinfo[j].indexconstraint = constrinfo[j].dobj.dumpId;

				/* If pre-7.3 DB, better make sure table comes first */
				addObjectDependency(&constrinfo[j].dobj,
									tbinfo->dobj.dumpId);
			}
			else
			{
				/* Plain secondary index */
				indxinfo[j].indexconstraint = 0;
			}
		}

		PQclear(res);
	}

	destroyPQExpBuffer(query);
}


/*
 * getConstraints
 *
 * Get info about constraints on dumpable tables.
 *
 * Currently handles foreign keys only.
 * Unique and primary key constraints are handled with indexes,
 * while check constraints are processed in getTableAttrs().
 */
/*	Declared in pg_dump.h */
void
getConstraints(TableInfo tblinfo[], int numTables)
{
	int			i,
				j;
	ConstraintInfo *constrinfo;
	PQExpBuffer query;
	PGresult   *res;
	int			i_condef,
				i_contableoid,
				i_conoid,
				i_conname;
	int			ntups;

	query = createPQExpBuffer();

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		if (tbinfo->ntrig == 0 || !tbinfo->dobj.dump)
			continue;

		if (g_verbose)
			mpp_err_msg(logInfo, progname, "reading foreign key constraints for table \"%s\"\n",
						tbinfo->dobj.name);

		/*
		 * select table schema to ensure constraint expr is qualified if
		 * needed
		 */
		selectSourceSchema(tbinfo->dobj.namespace->dobj.name);

		resetPQExpBuffer(query);
		appendPQExpBuffer(query,
						  "SELECT tableoid, oid, conname, "
						  "pg_catalog.pg_get_constraintdef(oid) as condef "
						  "FROM pg_catalog.pg_constraint "
						  "WHERE conrelid = '%u'::pg_catalog.oid "
						  "AND contype = 'f'",
						  tbinfo->dobj.catId.oid);
		res = PQexec(g_conn, query->data);
		check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

		ntups = PQntuples(res);

		i_contableoid = PQfnumber(res, "tableoid");
		i_conoid = PQfnumber(res, "oid");
		i_conname = PQfnumber(res, "conname");
		i_condef = PQfnumber(res, "condef");

		constrinfo = (ConstraintInfo *) malloc(ntups * sizeof(ConstraintInfo));

		for (j = 0; j < ntups; j++)
		{
			constrinfo[j].dobj.objType = DO_FK_CONSTRAINT;
			constrinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_contableoid));
			constrinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_conoid));
			AssignDumpId(&constrinfo[j].dobj);
			constrinfo[j].dobj.name = strdup(PQgetvalue(res, j, i_conname));
			constrinfo[j].dobj.namespace = tbinfo->dobj.namespace;
			constrinfo[j].contable = tbinfo;
			constrinfo[j].condomain = NULL;
			constrinfo[j].contype = 'f';
			constrinfo[j].condef = strdup(PQgetvalue(res, j, i_condef));
			constrinfo[j].conindex = 0;
			constrinfo[j].coninherited = false;
			constrinfo[j].separate = true;
		}

		PQclear(res);
	}

	destroyPQExpBuffer(query);
}


/*
 * getRules
 *	  get basic information about every rule in the system
 *
 * numRules is set to the number of rules read in
 */
RuleInfo *
getRules(int *numRules)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	RuleInfo   *ruleinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_rulename;
	int			i_ruletable;
	int			i_ev_type;
	int			i_is_instead;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	appendPQExpBuffer(query, "SELECT "
					  "tableoid, oid, rulename, "
					  "ev_class as ruletable, ev_type, is_instead "
					  "FROM pg_rewrite "
					  "ORDER BY oid");

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	*numRules = ntups;

	ruleinfo = (RuleInfo *) malloc(ntups * sizeof(RuleInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_rulename = PQfnumber(res, "rulename");
	i_ruletable = PQfnumber(res, "ruletable");
	i_ev_type = PQfnumber(res, "ev_type");
	i_is_instead = PQfnumber(res, "is_instead");

	for (i = 0; i < ntups; i++)
	{
		Oid			ruletableoid;

		ruleinfo[i].dobj.objType = DO_RULE;
		ruleinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		ruleinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&ruleinfo[i].dobj);
		ruleinfo[i].dobj.name = strdup(PQgetvalue(res, i, i_rulename));
		ruletableoid = atooid(PQgetvalue(res, i, i_ruletable));
		ruleinfo[i].ruletable = findTableByOid(ruletableoid);
		if (ruleinfo[i].ruletable == NULL)
		{
			mpp_err_msg(logError, progname, "failed sanity check, parent table OID %u of pg_rewrite entry OID %u not found\n",
						ruletableoid,
						ruleinfo[i].dobj.catId.oid);
			exit_nicely();
		}
		ruleinfo[i].dobj.namespace = ruleinfo[i].ruletable->dobj.namespace;
		ruleinfo[i].dobj.dump = ruleinfo[i].ruletable->dobj.dump;
		ruleinfo[i].ev_type = *(PQgetvalue(res, i, i_ev_type));
		ruleinfo[i].is_instead = *(PQgetvalue(res, i, i_is_instead)) == 't';
		if (ruleinfo[i].ruletable)
		{
			/*
			 * If the table is a view, force its ON SELECT rule to be sorted
			 * before the view itself --- this ensures that any dependencies
			 * for the rule affect the table's positioning. Other rules are
			 * forced to appear after their table.
			 */
			if (ruleinfo[i].ruletable->relkind == RELKIND_VIEW &&
				ruleinfo[i].ev_type == '1' && ruleinfo[i].is_instead)
			{
				addObjectDependency(&ruleinfo[i].ruletable->dobj,
									ruleinfo[i].dobj.dumpId);
				/* We'll merge the rule into CREATE VIEW, if possible */
				ruleinfo[i].separate = false;
			}
			else
			{
				addObjectDependency(&ruleinfo[i].dobj,
									ruleinfo[i].ruletable->dobj.dumpId);
				ruleinfo[i].separate = true;
			}
		}
		else
			ruleinfo[i].separate = true;
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return ruleinfo;
}


/*
 * getTriggers
 *	  get information about every trigger on a dumpable table
 *
 * Note: trigger data is not returned directly to the caller, but it
 * does get entered into the DumpableObject tables.
 */
void
getTriggers(TableInfo tblinfo[], int numTables)
{
	int			i,
				j;
	PQExpBuffer query = createPQExpBuffer();
	PGresult   *res;
	TriggerInfo *tginfo;
	int			i_tableoid,
				i_oid,
				i_tgname,
				i_tgfname,
				i_tgtype,
				i_tgnargs,
				i_tgargs,
				i_tgisconstraint,
				i_tgconstrname,
				i_tgconstrrelid,
				i_tgconstrrelname,
				i_tgenabled,
				i_tgdeferrable,
				i_tginitdeferred;
	int			ntups;

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		if (tbinfo->ntrig == 0 || !tbinfo->dobj.dump)
			continue;

		if (g_verbose)
			mpp_err_msg(logInfo, progname, "reading triggers for table \"%s\"\n",
						tbinfo->dobj.name);

		/*
		 * select table schema to ensure regproc name is qualified if needed
		 */
		selectSourceSchema(tbinfo->dobj.namespace->dobj.name);

		resetPQExpBuffer(query);

		/*
		 * We ignore triggers that are tied to a foreign-key constraint
		 */
		appendPQExpBuffer(query,
						  "SELECT tgname, "
						  "tgfoid::pg_catalog.regproc as tgfname, "
						  "tgtype, tgnargs, tgargs, tgenabled, "
						  "tgisconstraint, tgconstrname, tgdeferrable, "
						  "tgconstrrelid, tginitdeferred, tableoid, oid, "
					 "tgconstrrelid::pg_catalog.regclass as tgconstrrelname "
						  "from pg_catalog.pg_trigger t "
						  "where tgrelid = '%u'::pg_catalog.oid "
						  "and (not tgisconstraint "
						  " OR NOT EXISTS"
						  "  (SELECT 1 FROM pg_catalog.pg_depend d "
						  "   JOIN pg_catalog.pg_constraint c ON (d.refclassid = c.tableoid AND d.refobjid = c.oid) "
						  "   WHERE d.classid = t.tableoid AND d.objid = t.oid AND d.deptype = 'i' AND c.contype = 'f'))",
						  tbinfo->dobj.catId.oid);

		res = PQexec(g_conn, query->data);
		check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

		ntups = PQntuples(res);

		/*
		 * We may have less triggers than recorded due to having ignored
		 * foreign-key triggers
		 */
		if (ntups > tbinfo->ntrig)
		{
			mpp_err_msg(logError, progname, "expected %d triggers on table \"%s\" but found %d\n",
						tbinfo->ntrig, tbinfo->dobj.name, ntups);
			exit_nicely();
		}
		i_tableoid = PQfnumber(res, "tableoid");
		i_oid = PQfnumber(res, "oid");
		i_tgname = PQfnumber(res, "tgname");
		i_tgfname = PQfnumber(res, "tgfname");
		i_tgtype = PQfnumber(res, "tgtype");
		i_tgnargs = PQfnumber(res, "tgnargs");
		i_tgargs = PQfnumber(res, "tgargs");
		i_tgisconstraint = PQfnumber(res, "tgisconstraint");
		i_tgconstrname = PQfnumber(res, "tgconstrname");
		i_tgconstrrelid = PQfnumber(res, "tgconstrrelid");
		i_tgconstrrelname = PQfnumber(res, "tgconstrrelname");
		i_tgenabled = PQfnumber(res, "tgenabled");
		i_tgdeferrable = PQfnumber(res, "tgdeferrable");
		i_tginitdeferred = PQfnumber(res, "tginitdeferred");

		tginfo = (TriggerInfo *) malloc(ntups * sizeof(TriggerInfo));

		for (j = 0; j < ntups; j++)
		{
			tginfo[j].dobj.objType = DO_TRIGGER;
			tginfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_tableoid));
			tginfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_oid));
			AssignDumpId(&tginfo[j].dobj);
			tginfo[j].dobj.name = strdup(PQgetvalue(res, j, i_tgname));
			tginfo[j].dobj.namespace = tbinfo->dobj.namespace;
			tginfo[j].tgtable = tbinfo;
			tginfo[j].tgfname = strdup(PQgetvalue(res, j, i_tgfname));
			tginfo[j].tgtype = atoi(PQgetvalue(res, j, i_tgtype));
			tginfo[j].tgnargs = atoi(PQgetvalue(res, j, i_tgnargs));
			tginfo[j].tgargs = strdup(PQgetvalue(res, j, i_tgargs));
			tginfo[j].tgisconstraint = *(PQgetvalue(res, j, i_tgisconstraint)) == 't';
			tginfo[j].tgenabled = *(PQgetvalue(res, j, i_tgenabled)) == 't';
			tginfo[j].tgdeferrable = *(PQgetvalue(res, j, i_tgdeferrable)) == 't';
			tginfo[j].tginitdeferred = *(PQgetvalue(res, j, i_tginitdeferred)) == 't';

			if (tginfo[j].tgisconstraint)
			{
				tginfo[j].tgconstrname = strdup(PQgetvalue(res, j, i_tgconstrname));
				tginfo[j].tgconstrrelid = atooid(PQgetvalue(res, j, i_tgconstrrelid));
				if (OidIsValid(tginfo[j].tgconstrrelid))
				{
					if (PQgetisnull(res, j, i_tgconstrrelname))
					{
						mpp_err_msg(logError, progname, "query produced null referenced table name for foreign key trigger \"%s\" on table \"%s\" (OID of table: %u)\n",
									tginfo[j].dobj.name, tbinfo->dobj.name,
									tginfo[j].tgconstrrelid);
						exit_nicely();
					}
					tginfo[j].tgconstrrelname = strdup(PQgetvalue(res, j, i_tgconstrrelname));
				}
				else
					tginfo[j].tgconstrrelname = NULL;
			}
			else
			{
				tginfo[j].tgconstrname = NULL;
				tginfo[j].tgconstrrelid = InvalidOid;
				tginfo[j].tgconstrrelname = NULL;
			}
		}

		PQclear(res);
	}

	destroyPQExpBuffer(query);
}


/*
 * getProcLangs
 *	  get basic information about every procedural language in the system
 *
 * numProcLangs is set to the number of langs read in
 *
 * NB: this must run after getFuncs() because we assume we can do
 * findFuncByOid().
 */
ProcLangInfo *
getProcLangs(int *numProcLangs)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	ProcLangInfo *planginfo;
	int			i_tableoid;
	int			i_oid;
	int			i_lanname;
	int			i_lanpltrusted;
	int			i_lanplcallfoid;
	int			i_lanvalidator;
	int			i_lanacl;
	int			i_lanowner;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	/* Languages are owned by the bootstrap superuser, OID 10 */
	appendPQExpBuffer(query, "SELECT tableoid, oid, *, "
					  "(%s '10') as lanowner "
					  "FROM pg_language "
					  "WHERE lanispl "
					  "ORDER BY oid",
					  username_subquery);

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	*numProcLangs = ntups;

	planginfo = (ProcLangInfo *) malloc(ntups * sizeof(ProcLangInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_lanname = PQfnumber(res, "lanname");
	i_lanpltrusted = PQfnumber(res, "lanpltrusted");
	i_lanplcallfoid = PQfnumber(res, "lanplcallfoid");
	/* these may fail and return -1: */
	i_lanvalidator = PQfnumber(res, "lanvalidator");
	i_lanacl = PQfnumber(res, "lanacl");
	i_lanowner = PQfnumber(res, "lanowner");

	for (i = 0; i < ntups; i++)
	{
		planginfo[i].dobj.objType = DO_PROCLANG;
		planginfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		planginfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&planginfo[i].dobj);

		planginfo[i].dobj.name = strdup(PQgetvalue(res, i, i_lanname));
		planginfo[i].lanpltrusted = *(PQgetvalue(res, i, i_lanpltrusted)) == 't';
		planginfo[i].lanplcallfoid = atooid(PQgetvalue(res, i, i_lanplcallfoid));
		if (i_lanvalidator >= 0)
			planginfo[i].lanvalidator = atooid(PQgetvalue(res, i, i_lanvalidator));
		else
			planginfo[i].lanvalidator = InvalidOid;
		if (i_lanacl >= 0)
			planginfo[i].lanacl = strdup(PQgetvalue(res, i, i_lanacl));
		else
			planginfo[i].lanacl = strdup("{=U}");
		if (i_lanowner >= 0)
			planginfo[i].lanowner = strdup(PQgetvalue(res, i, i_lanowner));
		else
			planginfo[i].lanowner = strdup("");

	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return planginfo;
}


/*
 * getCasts
 *	  get basic information about every cast in the system
 *
 * numCasts is set to the number of casts read in
 */
CastInfo *
getCasts(int *numCasts)
{
	PGresult   *res;
	int			ntups;
	int			i;
	PQExpBuffer query = createPQExpBuffer();
	CastInfo   *castinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_castsource;
	int			i_casttarget;
	int			i_castfunc;
	int			i_castcontext;

	/* Make sure we are in proper schema */
	selectSourceSchema("pg_catalog");

	appendPQExpBuffer(query, "SELECT tableoid, oid, "
					  "castsource, casttarget, castfunc, castcontext "
					  "FROM pg_cast ORDER BY 3,4");

	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	ntups = PQntuples(res);

	*numCasts = ntups;

	castinfo = (CastInfo *) malloc(ntups * sizeof(CastInfo));

	i_tableoid = PQfnumber(res, "tableoid");
	i_oid = PQfnumber(res, "oid");
	i_castsource = PQfnumber(res, "castsource");
	i_casttarget = PQfnumber(res, "casttarget");
	i_castfunc = PQfnumber(res, "castfunc");
	i_castcontext = PQfnumber(res, "castcontext");

	for (i = 0; i < ntups; i++)
	{
		PQExpBufferData namebuf;
		TypeInfo   *sTypeInfo;
		TypeInfo   *tTypeInfo;

		castinfo[i].dobj.objType = DO_CAST;
		castinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
		castinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
		AssignDumpId(&castinfo[i].dobj);
		castinfo[i].castsource = atooid(PQgetvalue(res, i, i_castsource));
		castinfo[i].casttarget = atooid(PQgetvalue(res, i, i_casttarget));
		castinfo[i].castfunc = atooid(PQgetvalue(res, i, i_castfunc));
		castinfo[i].castcontext = *(PQgetvalue(res, i, i_castcontext));

		/*
		 * Try to name cast as concatenation of typnames.  This is only used
		 * for purposes of sorting.  If we fail to find either type, the name
		 * will be an empty string.
		 */
		initPQExpBuffer(&namebuf);
		sTypeInfo = findTypeByOid(castinfo[i].castsource);
		tTypeInfo = findTypeByOid(castinfo[i].casttarget);
		if (sTypeInfo && tTypeInfo)
			appendPQExpBuffer(&namebuf, "%s %s",
							  sTypeInfo->dobj.name, tTypeInfo->dobj.name);
		castinfo[i].dobj.name = namebuf.data;

		if (OidIsValid(castinfo[i].castfunc))
		{
			/*
			 * We need to make a dependency to ensure the function will be
			 * dumped first.  (In 7.3 and later the regular dependency
			 * mechanism will handle this for us.)
			 */
			FuncInfo   *funcInfo;

			funcInfo = findFuncByOid(castinfo[i].castfunc);
			if (funcInfo)
				addObjectDependency(&castinfo[i].dobj,
									funcInfo->dobj.dumpId);
		}
	}

	PQclear(res);

	destroyPQExpBuffer(query);

	return castinfo;
}


/*
 * getTableAttrs -
 *	  for each interesting table, read info about its attributes
 *	  (names, types, default values, CHECK constraints, etc)
 *
 * This is implemented in a very inefficient way right now, looping
 * through the tblinfo and doing a join per table to find the attrs and their
 * types.  However, because we want type names and so forth to be named
 * relative to the schema of each table, we couldn't do it in just one
 * query.  (Maybe one query per schema?)
 *
 *	modifies tblinfo
 */
void
getTableAttrs(TableInfo *tblinfo, int numTables)
{
	int			i,
				j;
	PQExpBuffer q = createPQExpBuffer();
	int			i_attnum;
	int			i_attname;
	int			i_atttypname;
	int			i_atttypmod;
	int			i_attstattarget;
	int			i_attstorage;
	int			i_typstorage;
	int			i_attnotnull;
	int			i_atthasdef;
	int			i_attisdropped;
	int			i_attislocal;
	int     i_attencoding;
	PGresult   *res;
	int			ntups;
	bool		hasdefaults;

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		/* Don't bother to collect info for sequences */
		if (tbinfo->relkind == RELKIND_SEQUENCE)
			continue;

		/* Don't bother with uninteresting tables, either */
		if (!tbinfo->interesting)
			continue;

		/*
		 * Make sure we are in proper schema for this table; this allows
		 * correct retrieval of formatted type names and default exprs
		 */
		selectSourceSchema(tbinfo->dobj.namespace->dobj.name);

		/* find all the user attributes and their types */

		/*
		 * we must read the attribute names in attribute number order! because
		 * we will use the attnum to index into the attnames array later.  We
		 * actually ask to order by "attrelid, attnum" because (at least up to
		 * 7.3) the planner is not smart enough to realize it needn't re-sort
		 * the output of an indexscan on pg_attribute_relid_attnum_index.
		 */
		if (g_verbose)
			mpp_err_msg(logInfo, progname, "finding the columns and types of table \"%s\"\n",
						tbinfo->dobj.name);

		resetPQExpBuffer(q);

		/* need left join here to not fail on dropped columns ... */
		appendPQExpBuffer(q, "SELECT a.attnum, a.attname, a.atttypmod, a.attstattarget, a.attstorage, t.typstorage, "
				  "a.attnotnull, a.atthasdef, a.attisdropped, a.attislocal, "
				   "pg_catalog.format_type(t.oid,a.atttypmod) as atttypname ");
		if (g_gp_supportsAttributeEncoding)
			appendPQExpBuffer(q, ", pg_catalog.array_to_string(e.attoptions, ',') as attencoding ");
		appendPQExpBuffer(q, "from pg_catalog.pg_attribute a left join pg_catalog.pg_type t "
						  "on a.atttypid = t.oid ");
		if (g_gp_supportsAttributeEncoding)
			appendPQExpBuffer(q, "	 LEFT OUTER JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum ");
		appendPQExpBuffer(q, "where a.attrelid = '%u'::pg_catalog.oid "
						  "and a.attnum > 0::pg_catalog.int2 "
						  "order by a.attrelid, a.attnum",
						  tbinfo->dobj.catId.oid);

		res = PQexec(g_conn, q->data);
		check_sql_result(res, g_conn, q->data, PGRES_TUPLES_OK);

		ntups = PQntuples(res);

		i_attnum = PQfnumber(res, "attnum");
		i_attname = PQfnumber(res, "attname");
		i_atttypname = PQfnumber(res, "atttypname");
		i_atttypmod = PQfnumber(res, "atttypmod");
		i_attstattarget = PQfnumber(res, "attstattarget");
		i_attstorage = PQfnumber(res, "attstorage");
		i_typstorage = PQfnumber(res, "typstorage");
		i_attnotnull = PQfnumber(res, "attnotnull");
		i_atthasdef = PQfnumber(res, "atthasdef");
		i_attisdropped = PQfnumber(res, "attisdropped");
		i_attislocal = PQfnumber(res, "attislocal");
		i_attencoding = PQfnumber(res, "attencoding");

		tbinfo->numatts = ntups;
		tbinfo->attnames = (char **) malloc(ntups * sizeof(char *));
		tbinfo->atttypnames = (char **) malloc(ntups * sizeof(char *));
		tbinfo->atttypmod = (int *) malloc(ntups * sizeof(int));
		tbinfo->attstattarget = (int *) malloc(ntups * sizeof(int));
		tbinfo->attstorage = (char *) malloc(ntups * sizeof(char));
		tbinfo->typstorage = (char *) malloc(ntups * sizeof(char));
		tbinfo->attisdropped = (bool *) malloc(ntups * sizeof(bool));
		tbinfo->attislocal = (bool *) malloc(ntups * sizeof(bool));
		tbinfo->notnull = (bool *) malloc(ntups * sizeof(bool));
		tbinfo->attrdefs = (AttrDefInfo **) malloc(ntups * sizeof(AttrDefInfo *));
		tbinfo->inhAttrs = (bool *) malloc(ntups * sizeof(bool));
		tbinfo->inhAttrDef = (bool *) malloc(ntups * sizeof(bool));
		tbinfo->inhNotNull = (bool *) malloc(ntups * sizeof(bool));
		tbinfo->attencoding = (char **)malloc(ntups * sizeof(char *));
		hasdefaults = false;

		for (j = 0; j < ntups; j++)
		{
			if (j + 1 != atoi(PQgetvalue(res, j, i_attnum)))
			{
				mpp_err_msg(logError, progname, "invalid column numbering in table \"%s\"\n",
							tbinfo->dobj.name);
				exit_nicely();
			}
			tbinfo->attnames[j] = strdup(PQgetvalue(res, j, i_attname));
			tbinfo->atttypnames[j] = strdup(PQgetvalue(res, j, i_atttypname));
			tbinfo->atttypmod[j] = atoi(PQgetvalue(res, j, i_atttypmod));
			tbinfo->attstattarget[j] = atoi(PQgetvalue(res, j, i_attstattarget));
			tbinfo->attstorage[j] = *(PQgetvalue(res, j, i_attstorage));
			tbinfo->typstorage[j] = *(PQgetvalue(res, j, i_typstorage));
			tbinfo->attisdropped[j] = (PQgetvalue(res, j, i_attisdropped)[0] == 't');
			tbinfo->attislocal[j] = (PQgetvalue(res, j, i_attislocal)[0] == 't');
			tbinfo->notnull[j] = (PQgetvalue(res, j, i_attnotnull)[0] == 't');
			tbinfo->attrdefs[j] = NULL; /* fix below */
			if (PQgetvalue(res, j, i_atthasdef)[0] == 't')
				hasdefaults = true;
			/* these flags will be set in flagInhAttrs() */
			tbinfo->inhAttrs[j] = false;
			tbinfo->inhAttrDef[j] = false;
			tbinfo->inhNotNull[j] = false;

			/* column storage attributes */
			if (g_gp_supportsAttributeEncoding && !PQgetisnull(res, j, i_attencoding))
				tbinfo->attencoding[j] = strdup(PQgetvalue(res, j, i_attencoding));
			else
				tbinfo->attencoding[j] = NULL;
		}

		PQclear(res);

		/*
		 * Get info about column defaults
		 */
		if (hasdefaults)
		{
			AttrDefInfo *attrdefs;
			int			numDefaults;

			if (g_verbose)
				mpp_err_msg(logInfo, progname, "finding default expressions of table \"%s\"\n",
							tbinfo->dobj.name);

			resetPQExpBuffer(q);
			appendPQExpBuffer(q, "SELECT tableoid, oid, adnum, "
						   "pg_catalog.pg_get_expr(adbin, adrelid) AS adsrc "
							  "FROM pg_catalog.pg_attrdef "
							  "WHERE adrelid = '%u'::pg_catalog.oid",
							  tbinfo->dobj.catId.oid);
			res = PQexec(g_conn, q->data);
			check_sql_result(res, g_conn, q->data, PGRES_TUPLES_OK);

			numDefaults = PQntuples(res);
			attrdefs = (AttrDefInfo *) malloc(numDefaults * sizeof(AttrDefInfo));

			for (j = 0; j < numDefaults; j++)
			{
				int			adnum;

				attrdefs[j].dobj.objType = DO_ATTRDEF;
				attrdefs[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, 0));
				attrdefs[j].dobj.catId.oid = atooid(PQgetvalue(res, j, 1));
				AssignDumpId(&attrdefs[j].dobj);
				attrdefs[j].adtable = tbinfo;
				attrdefs[j].adnum = adnum = atoi(PQgetvalue(res, j, 2));
				attrdefs[j].adef_expr = strdup(PQgetvalue(res, j, 3));

				attrdefs[j].dobj.name = strdup(tbinfo->dobj.name);
				attrdefs[j].dobj.namespace = tbinfo->dobj.namespace;

				attrdefs[j].dobj.dump = tbinfo->dobj.dump;

				/*
				 * Defaults on a VIEW must always be dumped as separate ALTER
				 * TABLE commands.	Defaults on regular tables are dumped as
				 * part of the CREATE TABLE if possible.  To check if it's
				 * safe, we mark the default as needing to appear before the
				 * CREATE.
				 */
				if (tbinfo->relkind == RELKIND_VIEW)
				{
					attrdefs[j].separate = true;
					/* needed in case pre-7.3 DB: */
					addObjectDependency(&attrdefs[j].dobj,
										tbinfo->dobj.dumpId);
				}
				else
				{
					attrdefs[j].separate = false;
					addObjectDependency(&tbinfo->dobj,
										attrdefs[j].dobj.dumpId);
				}

				if (adnum <= 0 || adnum > ntups)
				{
					mpp_err_msg(logError, progname, "invalid adnum value %d for table \"%s\"\n",
								adnum, tbinfo->dobj.name);
					exit_nicely();
				}
				tbinfo->attrdefs[adnum - 1] = &attrdefs[j];
			}
			PQclear(res);
		}

		/*
		 * Get info about table CHECK constraints
		 */
		if (tbinfo->ncheck > 0)
		{
			ConstraintInfo *constrs;
			int			numConstrs;

			if (g_verbose)
				mpp_err_msg(logInfo, progname, "finding check constraints for table \"%s\"\n",
							tbinfo->dobj.name);

			resetPQExpBuffer(q);
			appendPQExpBuffer(q, "SELECT tableoid, oid, conname, "
							"pg_catalog.pg_get_constraintdef(oid) AS consrc "
							  "FROM pg_catalog.pg_constraint "
							  "WHERE conrelid = '%u'::pg_catalog.oid "
							  "   AND contype = 'c' "
							  "ORDER BY conname",
							  tbinfo->dobj.catId.oid);
			res = PQexec(g_conn, q->data);
			check_sql_result(res, g_conn, q->data, PGRES_TUPLES_OK);

			numConstrs = PQntuples(res);
			if (numConstrs != tbinfo->ncheck)
			{
				mpp_err_msg(logError, progname, "expected %d check constraints on table \"%s\" but found %d\n",
							tbinfo->ncheck, tbinfo->dobj.name, numConstrs);
				mpp_err_msg(logError, progname, "(The system catalogs might be corrupted.)\n");
				exit_nicely();
			}

			constrs = (ConstraintInfo *) malloc(numConstrs * sizeof(ConstraintInfo));
			tbinfo->checkexprs = constrs;

			for (j = 0; j < numConstrs; j++)
			{
				constrs[j].dobj.objType = DO_CONSTRAINT;
				constrs[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, 0));
				constrs[j].dobj.catId.oid = atooid(PQgetvalue(res, j, 1));
				AssignDumpId(&constrs[j].dobj);
				constrs[j].dobj.name = strdup(PQgetvalue(res, j, 2));
				constrs[j].dobj.namespace = tbinfo->dobj.namespace;
				constrs[j].contable = tbinfo;
				constrs[j].condomain = NULL;
				constrs[j].contype = 'c';
				constrs[j].condef = strdup(PQgetvalue(res, j, 3));
				constrs[j].conindex = 0;
				constrs[j].coninherited = false;
				constrs[j].separate = false;

				constrs[j].dobj.dump = tbinfo->dobj.dump;

				/*
				 * Mark the constraint as needing to appear before the table
				 * --- this is so that any other dependencies of the
				 * constraint will be emitted before we try to create the
				 * table.
				 */
				addObjectDependency(&tbinfo->dobj,
									constrs[j].dobj.dumpId);

				/*
				 * If the constraint is inherited, this will be detected
				 * later.  We also detect later if the constraint must be
				 * split out from the table definition.
				 */
			}
			PQclear(res);
		}
	}

	destroyPQExpBuffer(q);
}

/*
 * testSqlMedSupport - tests whether or not the current GP database includes
 * support for SQL/MED and foreign tables.
 */
bool
testSqlMedSupport(void)
{
	PQExpBuffer query;
	PGresult   *res;
	bool		isSupported;

	query = createPQExpBuffer();

	appendPQExpBuffer(query, "SELECT 1 FROM pg_class WHERE relname = 'pg_foreign_server' and relnamespace = 11;");
	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	isSupported = (PQntuples(res) == 1);

	PQclear(res);
	destroyPQExpBuffer(query);

	return isSupported;
}

bool
testExtProtocolSupport(void)
{
	PQExpBuffer query;
	PGresult   *res;
	bool		isSupported;

	query = createPQExpBuffer();

	appendPQExpBuffer(query, "SELECT 1 FROM pg_class WHERE relname = 'pg_extprotocol' and relnamespace = 11;");
	res = PQexec(g_conn, query->data);
	check_sql_result(res, g_conn, query->data, PGRES_TUPLES_OK);

	isSupported = (PQntuples(res) == 1);

	PQclear(res);
	destroyPQExpBuffer(query);

	return isSupported;
}
