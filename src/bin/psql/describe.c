/*
 * psql - the PostgreSQL interactive terminal
 *
 * Support for the various \d ("describe") commands.  Note that the current
 * expectation is that all functions in this file will succeed when working
 * with servers of versions 7.4 and up.  It's okay to omit irrelevant
 * information for an old server, but not to fail outright.
 *
 * Copyright (c) 2000-2010, PostgreSQL Global Development Group
 *
 * src/bin/psql/describe.c
 */
#include "postgres_fe.h"

#include <ctype.h>

#include "common.h"
#include "describe.h"
#include "dumputils.h"
#include "mbprint.h"
#include "print.h"
#include "settings.h"
#include "variables.h"


static bool describeOneTableDetails(const char *schemaname,
						const char *relationname,
						const char *oid,
						bool verbose);
static int add_distributed_by_footer(const char* oid, PQExpBufferData *inoutbuf, PQExpBufferData buf);
static int add_partition_by_footer(const char* oid, PQExpBufferData *inoutbuf, PQExpBufferData *buf);
static void add_tablespace_footer(printTableContent *const cont, char relkind,
					  Oid tablespace, const bool newline);
static void add_role_attribute(PQExpBuffer buf, const char *const str);
static bool listTSParsersVerbose(const char *pattern);
static bool describeOneTSParser(const char *oid, const char *nspname,
					const char *prsname);
static bool listTSConfigsVerbose(const char *pattern);
static bool describeOneTSConfig(const char *oid, const char *nspname,
					const char *cfgname,
					const char *pnspname, const char *prsname);
static void printACLColumn(PQExpBuffer buf, const char *colname);
static bool isGPDB(void);
static bool isGPDB4200OrLater(void);

/* GPDB 3.2 used PG version 8.2.10, and we've moved the minor number up since then for each release,  4.1 = 8.2.15 */
/* Allow for a couple of future releases.  If the version isn't in this range, we are talking to PostgreSQL, not GPDB */
#define mightBeGPDB() (pset.sversion >= 80210 && pset.sversion < 80222)

static bool isGPDB(void)
{
	static enum { gpdb_maybe, gpdb_yes, gpdb_no } talking_to_gpdb;
	if (mightBeGPDB())
	{
		PGresult   *res;
		char       *ver;

		if (talking_to_gpdb == gpdb_yes)
			return true;
		else if (talking_to_gpdb == gpdb_no)
			return false;

		res = PSQLexec("select version()", false);
		if (!res)
			return false;

		ver = PQgetvalue(res, 0, 0);
		if (strstr(ver,"Greenplum") != NULL)
		{
			PQclear(res);
			talking_to_gpdb = gpdb_yes;
			return true;
		}

		talking_to_gpdb = gpdb_no;
		PQclear(res);
	}

	talking_to_gpdb = gpdb_maybe; /* if we reconnect to a GPDB system later. do the check again */

	return false;
}


/*
 * A new catalog table was introduced in GPDB 4.2 (pg_catalog.pg_attribute_encoding).
 * If the database appears to be GPDB and has that table, we assume it is 4.2 or later.
 *
 * Return true if GPDB version 4.2 or later, false otherwise.
 */
static bool isGPDB4200OrLater(void)
{
	bool       retValue = false;

	if (isGPDB() == true)
	{
		PGresult  *result;

		result = PSQLexec("select oid from pg_catalog.pg_class where relnamespace = 11 and relname  = 'pg_attribute_encoding'", false);
		if (PQgetisnull(result, 0, 0))
			 retValue = false;
		else
			retValue = true;
	}
	return retValue;
}

/*
 * Returns true if HAWQ version 1.1 or later, false otherwise.
 */
static bool
isHAWQ1100OrLater(void)
{
	bool       retValue = false;

	if (isGPDB() == true)
	{
		PGresult  *result;

		result = PSQLexec(
				"select attnum from pg_catalog.pg_attribute "
				"where attrelid = 'pg_catalog.pg_proc'::regclass and "
				"attname = 'prodataaccess'", false);
		retValue = PQntuples(result) > 0;
	}
	return retValue;
}

/*----------------
 * Handlers for various slash commands displaying some sort of list
 * of things in the database.
 *
 * Note: try to format the queries to look nice in -E output.
 *----------------
 */


/* \da
 * Takes an optional regexp to select particular aggregates
 */
bool
describeAggregates(const char *pattern, bool verbose, bool showSystem)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT n.nspname as \"%s\",\n"
					  "  p.proname AS \"%s\",\n"
				 "  pg_catalog.format_type(p.prorettype, NULL) AS \"%s\",\n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"),
					  gettext_noop("Result data type"));

	if (pset.sversion >= 80200)
		appendPQExpBuffer(&buf,
						  "  CASE WHEN p.pronargs = 0\n"
						  "    THEN CAST('*' AS pg_catalog.text)\n"
						  "    ELSE\n"
						  "    pg_catalog.array_to_string(ARRAY(\n"
						  "      SELECT\n"
				 "        pg_catalog.format_type(p.proargtypes[s.i], NULL)\n"
						  "      FROM\n"
						  "        pg_catalog.generate_series(0, pg_catalog.array_upper(p.proargtypes, 1)) AS s(i)\n"
						  "    ), ', ')\n"
						  "  END AS \"%s\",\n",
						  gettext_noop("Argument data types"));
	else
		appendPQExpBuffer(&buf,
			 "  pg_catalog.format_type(p.proargtypes[0], NULL) AS \"%s\",\n",
						  gettext_noop("Argument data types"));

	appendPQExpBuffer(&buf,
				 "  pg_catalog.obj_description(p.oid, 'pg_proc') as \"%s\"\n"
					  "FROM pg_catalog.pg_proc p\n"
	   "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n"
					  "WHERE p.proisagg\n",
					  gettext_noop("Description"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "p.proname", NULL,
						  "pg_catalog.pg_function_is_visible(p.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2, 4;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of aggregate functions");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}

/* \db
 * Takes an optional regexp to select particular tablespaces
 */
bool
describeTablespaces(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	if (pset.sversion < 80000)
	{
		fprintf(stderr, _("The server (version %d.%d) does not support tablespaces.\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	initPQExpBuffer(&buf);

    if (isGPDB() && pset.sversion >= 80213) /* GPDB ? */
	printfPQExpBuffer(&buf,
					  "SELECT spcname AS \"%s\",\n"
					  "  pg_catalog.pg_get_userbyid(spcowner) AS \"%s\",\n"
					  "  fsname AS \"%s\"",
					  gettext_noop("Name"),
					  gettext_noop("Owner"),
					  gettext_noop("Filespae Name"));
    else
    printfPQExpBuffer(&buf,
					  "SELECT spcname AS \"%s\",\n"
					  "  pg_catalog.pg_get_userbyid(spcowner) AS \"%s\",\n"
					  "  spclocation AS \"%s\"",
					  gettext_noop("Name"),
					  gettext_noop("Owner"),
					  gettext_noop("Location"));

	if (verbose)
	{
		appendPQExpBuffer(&buf, ",\n  ");
		printACLColumn(&buf, "spcacl");
	}

	if (verbose && pset.sversion >= 80200)
		appendPQExpBuffer(&buf,
		 ",\n  pg_catalog.shobj_description(t.oid, 'pg_tablespace') AS \"%s\"",
						  gettext_noop("Description"));

	appendPQExpBuffer(&buf,
					  "\nFROM pg_catalog.pg_tablespace t\n");
	if (isGPDB())
	    appendPQExpBuffer(&buf,
					  "JOIN pg_catalog.pg_filespace fs on (spcfsoid=fs.oid)");

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  NULL, "spcname", NULL,
						  NULL);

	appendPQExpBuffer(&buf, "ORDER BY 1;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of tablespaces");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}


/* \df
 * Takes an optional regexp to select particular functions.
 *
 * As with \d, you can specify the kinds of functions you want:
 *
 * a for aggregates
 * n for normal
 * t for trigger
 * w for window
 *
 * and you can mix and match these in any order.
 */
bool
describeFunctions(const char *functypes, const char *pattern, bool verbose, bool showSystem)
{
	bool		showAggregate = strchr(functypes, 'a') != NULL;
	bool		showNormal = strchr(functypes, 'n') != NULL;
	bool		showTrigger = strchr(functypes, 't') != NULL;
	bool		showWindow = strchr(functypes, 'w') != NULL;
	bool		have_where;
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;
	static const bool translate_columns[] = {false, false, false, false, true, true, false, false, false, false};

	if (strlen(functypes) != strspn(functypes, "antwS+"))
	{
		fprintf(stderr, _("\\df only takes [antwS+] as options\n"));
		return true;
	}

	if (showWindow && pset.sversion < 80400)
	{
		fprintf(stderr, _("\\df does not take a \"w\" option with server version %d.%d\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	if (!showAggregate && !showNormal && !showTrigger && !showWindow)
	{
		showAggregate = showNormal = showTrigger = true;
		if (pset.sversion >= 80400)
			showWindow = true;
	}

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT n.nspname as \"%s\",\n"
					  "  p.proname as \"%s\",\n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"));

	if (pset.sversion >= 80400)
		appendPQExpBuffer(&buf,
						  "  pg_catalog.pg_get_function_result(p.oid) as \"%s\",\n"
						  "  pg_catalog.pg_get_function_arguments(p.oid) as \"%s\",\n"
						  " CASE\n"
						  "  WHEN p.proisagg THEN '%s'\n"
						  "  WHEN p.proiswindow THEN '%s'\n"
						  "  WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN '%s'\n"
						  "  ELSE '%s'\n"
						  "END as \"%s\"",
						  gettext_noop("Result data type"),
						  gettext_noop("Argument data types"),
		/* translator: "agg" is short for "aggregate" */
						  gettext_noop("agg"),
						  gettext_noop("window"),
						  gettext_noop("trigger"),
						  gettext_noop("normal"),
						  gettext_noop("Type"));
	else if (pset.sversion >= 80100)
		appendPQExpBuffer(&buf,
						  "  CASE WHEN p.proretset THEN 'SETOF ' ELSE '' END ||\n"
						  "  pg_catalog.format_type(p.prorettype, NULL) as \"%s\",\n"
						  "  CASE WHEN proallargtypes IS NOT NULL THEN\n"
						  "    pg_catalog.array_to_string(ARRAY(\n"
						  "      SELECT\n"
						  "        CASE\n"
						  "          WHEN p.proargmodes[s.i] = 'i' THEN ''\n"
						  "          WHEN p.proargmodes[s.i] = 'o' THEN 'OUT '\n"
						  "          WHEN p.proargmodes[s.i] = 'b' THEN 'INOUT '\n"
						  "          WHEN p.proargmodes[s.i] = 'v' THEN 'VARIADIC '\n"
						  "        END ||\n"
						  "        CASE\n"
						  "          WHEN COALESCE(p.proargnames[s.i], '') = '' THEN ''\n"
						  "          ELSE p.proargnames[s.i] || ' ' \n"
						  "        END ||\n"
						  "        pg_catalog.format_type(p.proallargtypes[s.i], NULL)\n"
						  "      FROM\n"
						  "        pg_catalog.generate_series(1, pg_catalog.array_upper(p.proallargtypes, 1)) AS s(i)\n"
						  "    ), ', ')\n"
						  "  ELSE\n"
						  "    pg_catalog.array_to_string(ARRAY(\n"
						  "      SELECT\n"
						  "        CASE\n"
						  "          WHEN COALESCE(p.proargnames[s.i+1], '') = '' THEN ''\n"
						  "          ELSE p.proargnames[s.i+1] || ' '\n"
						  "          END ||\n"
						  "        pg_catalog.format_type(p.proargtypes[s.i], NULL)\n"
						  "      FROM\n"
						  "        pg_catalog.generate_series(0, pg_catalog.array_upper(p.proargtypes, 1)) AS s(i)\n"
						  "    ), ', ')\n"
						  "  END AS \"%s\",\n"
						  "  CASE\n"
						  "    WHEN p.proisagg THEN '%s'\n"
						  "    WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN '%s'\n"
						  "    ELSE '%s'\n"
						  "  END AS \"%s\"",
						  gettext_noop("Result data type"),
						  gettext_noop("Argument data types"),
		/* translator: "agg" is short for "aggregate" */
						  gettext_noop("agg"),
						  gettext_noop("trigger"),
						  gettext_noop("normal"),
						  gettext_noop("Type"));
	else
		appendPQExpBuffer(&buf,
					 "  CASE WHEN p.proretset THEN 'SETOF ' ELSE '' END ||\n"
				  "  pg_catalog.format_type(p.prorettype, NULL) as \"%s\",\n"
					"  pg_catalog.oidvectortypes(p.proargtypes) as \"%s\",\n"
						  "  CASE\n"
						  "    WHEN p.proisagg THEN '%s'\n"
						  "    WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN '%s'\n"
						  "    ELSE '%s'\n"
						  "  END AS \"%s\"",
						  gettext_noop("Result data type"),
						  gettext_noop("Argument data types"),
		/* translator: "agg" is short for "aggregate" */
						  gettext_noop("agg"),
						  gettext_noop("trigger"),
						  gettext_noop("normal"),
						  gettext_noop("Type"));

	if (verbose)
	{
		if (isHAWQ1100OrLater())
			appendPQExpBuffer(&buf,
						  ",\n CASE\n"
						  "  WHEN p.prodataaccess = 'n' THEN '%s'\n"
						  "  WHEN p.prodataaccess = 'c' THEN '%s'\n"
						  "  WHEN p.prodataaccess = 'r' THEN '%s'\n"
						  "  WHEN p.prodataaccess = 'm' THEN '%s'\n"
						  "END as \"%s\"",
						  gettext_noop("no sql"),
						  gettext_noop("contains sql"),
						  gettext_noop("reads sql data"),
						  gettext_noop("modifies sql data"),
						  gettext_noop("Data access"));
		appendPQExpBuffer(&buf,
						  ",\n CASE\n"
						  "  WHEN p.provolatile = 'i' THEN '%s'\n"
						  "  WHEN p.provolatile = 's' THEN '%s'\n"
						  "  WHEN p.provolatile = 'v' THEN '%s'\n"
						  "END as \"%s\""
				   ",\n  pg_catalog.pg_get_userbyid(p.proowner) as \"%s\",\n"
						  "  l.lanname as \"%s\",\n"
						  "  p.prosrc as \"%s\",\n"
				  "  pg_catalog.obj_description(p.oid, 'pg_proc') as \"%s\"",
						  gettext_noop("immutable"),
						  gettext_noop("stable"),
						  gettext_noop("volatile"),
						  gettext_noop("Volatility"),
						  gettext_noop("Owner"),
						  gettext_noop("Language"),
						  gettext_noop("Source code"),
						  gettext_noop("Description"));
	}

	appendPQExpBuffer(&buf,
					  "\nFROM pg_catalog.pg_proc p"
	"\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n");

	if (verbose)
		appendPQExpBuffer(&buf,
		   "     LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang\n");

	have_where = false;

	/* filter by function type, if requested */
	if (showNormal && showAggregate && showTrigger && showWindow)
		 /* Do nothing */ ;
	else if (showNormal)
	{
		if (!showAggregate)
		{
			if (have_where)
				appendPQExpBuffer(&buf, "      AND ");
			else
			{
				appendPQExpBuffer(&buf, "WHERE ");
				have_where = true;
			}
			appendPQExpBuffer(&buf, "NOT p.proisagg\n");
		}
		if (!showTrigger)
		{
			if (have_where)
				appendPQExpBuffer(&buf, "      AND ");
			else
			{
				appendPQExpBuffer(&buf, "WHERE ");
				have_where = true;
			}
			appendPQExpBuffer(&buf, "p.prorettype <> 'pg_catalog.trigger'::pg_catalog.regtype\n");
		}
		if (!showWindow && pset.sversion >= 80400)
		{
			if (have_where)
				appendPQExpBuffer(&buf, "      AND ");
			else
			{
				appendPQExpBuffer(&buf, "WHERE ");
				have_where = true;
			}
			appendPQExpBuffer(&buf, "NOT p.proiswindow\n");
		}
	}
	else
	{
		bool		needs_or = false;

		appendPQExpBuffer(&buf, "WHERE (\n       ");
		have_where = true;
		/* Note: at least one of these must be true ... */
		if (showAggregate)
		{
			appendPQExpBuffer(&buf, "p.proisagg\n");
			needs_or = true;
		}
		if (showTrigger)
		{
			if (needs_or)
				appendPQExpBuffer(&buf, "       OR ");
			appendPQExpBuffer(&buf,
				"p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype\n");
			needs_or = true;
		}
		if (showWindow)
		{
			if (needs_or)
				appendPQExpBuffer(&buf, "       OR ");
			appendPQExpBuffer(&buf, "p.proiswindow\n");
			needs_or = true;
		}
		appendPQExpBuffer(&buf, "      )\n");
	}

	processSQLNamePattern(pset.db, &buf, pattern, have_where, false,
						  "n.nspname", "p.proname", NULL,
						  "pg_catalog.pg_function_is_visible(p.oid)");

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2, 4;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of functions");
	myopt.translate_header = true;
	myopt.translate_columns = translate_columns;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}



/*
 * \dT
 * describe types
 */
bool
describeTypes(const char *pattern, bool verbose, bool showSystem)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;
	bool isGE42 = isGPDB4200OrLater();

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT n.nspname as \"%s\",\n"
					  "  pg_catalog.format_type(t.oid, NULL) AS \"%s\",\n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"));
	if (verbose)
	{
		appendPQExpBuffer(&buf,
						  "  t.typname AS \"%s\",\n"
						  "  CASE WHEN t.typrelid != 0\n"
						  "      THEN CAST('tuple' AS pg_catalog.text)\n"
						  "    WHEN t.typlen < 0\n"
						  "      THEN CAST('var' AS pg_catalog.text)\n"
						  "    ELSE CAST(t.typlen AS pg_catalog.text)\n"
						  "  END AS \"%s\",\n",
						  gettext_noop("Internal name"),
						  gettext_noop("Size"));
	}
	if (verbose && pset.sversion >= 80300)
		appendPQExpBuffer(&buf,
						  "  pg_catalog.array_to_string(\n"
						  "      ARRAY(\n"
						  "		     SELECT e.enumlabel\n"
						  "          FROM pg_catalog.pg_enum e\n"
						  "          WHERE e.enumtypid = t.oid\n"
						  "          ORDER BY e.oid\n"
						  "      ),\n"
						  "      E'\\n'\n"
						  "  ) AS \"%s\",\n",
						  gettext_noop("Elements"));
	if (verbose && isGE42 == true)
		appendPQExpBuffer(&buf, " pg_catalog.array_to_string(te.typoptions, ',') AS \"%s\",\n", gettext_noop("Type Options"));

	appendPQExpBuffer(&buf,
				"  pg_catalog.obj_description(t.oid, 'pg_type') as \"%s\"\n",
					  gettext_noop("Description"));

	appendPQExpBuffer(&buf, "FROM pg_catalog.pg_type t\n"
	 "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n");
	if (verbose && isGE42 == true)
		appendPQExpBuffer(&buf, "\n   LEFT OUTER JOIN pg_catalog.pg_type_encoding te ON te.typid = t.oid ");

	/*
	 * do not include complex types (typrelid!=0) unless they are standalone
	 * composite types
	 */
	appendPQExpBuffer(&buf, "WHERE (t.typrelid = 0 ");
	appendPQExpBuffer(&buf, "OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c "
					  "WHERE c.oid = t.typrelid))\n");

	/*
	 * do not include array types (before 8.3 we have to use the assumption
	 * that their names start with underscore)
	 */
	if (pset.sversion >= 80300)
		appendPQExpBuffer(&buf, "  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)\n");
	else
		appendPQExpBuffer(&buf, "  AND t.typname !~ '^_'\n");

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	/* Match name pattern against either internal or external name */
	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "t.typname",
						  "pg_catalog.format_type(t.oid, NULL)",
						  "pg_catalog.pg_type_is_visible(t.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of data types");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}


/* \do
 */
bool
describeOperators(const char *pattern, bool showSystem)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT n.nspname as \"%s\",\n"
					  "  o.oprname AS \"%s\",\n"
					  "  CASE WHEN o.oprkind='l' THEN NULL ELSE pg_catalog.format_type(o.oprleft, NULL) END AS \"%s\",\n"
					  "  CASE WHEN o.oprkind='r' THEN NULL ELSE pg_catalog.format_type(o.oprright, NULL) END AS \"%s\",\n"
				   "  pg_catalog.format_type(o.oprresult, NULL) AS \"%s\",\n"
			 "  coalesce(pg_catalog.obj_description(o.oid, 'pg_operator'),\n"
	"           pg_catalog.obj_description(o.oprcode, 'pg_proc')) AS \"%s\"\n"
					  "FROM pg_catalog.pg_operator o\n"
	  "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace\n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"),
					  gettext_noop("Left arg type"),
					  gettext_noop("Right arg type"),
					  gettext_noop("Result type"),
					  gettext_noop("Description"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "WHERE n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, !showSystem && !pattern, true,
						  "n.nspname", "o.oprname", NULL,
						  "pg_catalog.pg_operator_is_visible(o.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2, 3, 4;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of operators");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}


/*
 * listAllDbs
 *
 * for \l, \list, and -l switch
 */
bool
listAllDbs(bool verbose)
{
	PGresult   *res;
	PQExpBufferData buf;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT d.datname as \"%s\",\n"
				   "       pg_catalog.pg_get_userbyid(d.datdba) as \"%s\",\n"
			"       pg_catalog.pg_encoding_to_char(d.encoding) as \"%s\",\n",
					  gettext_noop("Name"),
					  gettext_noop("Owner"),
					  gettext_noop("Encoding"));
	if (pset.sversion >= 80400)
		appendPQExpBuffer(&buf,
						  "       d.datcollate as \"%s\",\n"
						  "       d.datctype as \"%s\",\n",
						  gettext_noop("Collation"),
						  gettext_noop("Ctype"));
	appendPQExpBuffer(&buf, "       ");
	printACLColumn(&buf, "d.datacl");
	if (verbose && pset.sversion >= 80200)
		appendPQExpBuffer(&buf,
						  ",\n       CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')\n"
						  "            THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname))\n"
						  "            ELSE 'No Access'\n"
						  "       END as \"%s\"",
						  gettext_noop("Size"));
	if (verbose && pset.sversion >= 80000)
		appendPQExpBuffer(&buf,
						  ",\n       t.spcname as \"%s\"",
						  gettext_noop("Tablespace"));
	if (verbose && pset.sversion >= 80200)
		appendPQExpBuffer(&buf,
						  ",\n       pg_catalog.shobj_description(d.oid, 'pg_database') as \"%s\"",
						  gettext_noop("Description"));
	appendPQExpBuffer(&buf,
					  "\nFROM pg_catalog.pg_database d\n");
	if (verbose && pset.sversion >= 80000)
		appendPQExpBuffer(&buf,
		   "  JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid\n");
	/* Exclude database hcatalog since it's not an internal database */
	appendPQExpBuffer(&buf, "WHERE d.datname <> 'hcatalog'\n");
	appendPQExpBuffer(&buf, "ORDER BY 1;");
	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of databases");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}


/*
 * List Tables' Grant/Revoke Permissions
 * \z (now also \dp -- perhaps more mnemonic)
 */
bool
permissionsList(const char *pattern)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;
	static const bool translate_columns[] = {false, false, true, false, false};

	initPQExpBuffer(&buf);

	/*
	 * we ignore indexes and toast tables since they have no meaningful rights
	 */
	printfPQExpBuffer(&buf,
					  "SELECT n.nspname as \"%s\",\n"
					  "  c.relname as \"%s\",\n"
					  "  CASE c.relkind WHEN 'r' THEN '%s' WHEN 'v' THEN '%s' WHEN 'S' THEN '%s' END as \"%s\",\n"
					  "  ",
					  gettext_noop("Schema"),
					  gettext_noop("Name"),
	   gettext_noop("table"), gettext_noop("view"), gettext_noop("sequence"),
					  gettext_noop("Type"));

	if (strcmp(get_line_style(&pset.popt.topt)->name,"old-ascii") != 0)
	{
	/*
	 * This new code, which produces a more correct and more useful answer, is disabled
	 * for the moment if the print format is old-ascii, because we'd need to upgrade the regression tests (cdbfast and make installcheck-good).
	 */
	printACLColumn(&buf, "c.relacl");
	}
	else
	appendPQExpBuffer(&buf, "c.relacl as \"Access privileges\"");  /* Old style, pre-8.3 */


	if (pset.sversion >= 80400)
		appendPQExpBuffer(&buf,
						  ",\n  pg_catalog.array_to_string(ARRAY(\n"
						  "    SELECT attname || E':\\n  ' || pg_catalog.array_to_string(attacl, E'\\n  ')\n"
						  "    FROM pg_catalog.pg_attribute a\n"
						  "    WHERE attrelid = c.oid AND NOT attisdropped AND attacl IS NOT NULL\n"
						  "  ), E'\\n') AS \"%s\"",
						  gettext_noop("Column access privileges"));

	appendPQExpBuffer(&buf, "\nFROM pg_catalog.pg_class c\n"
	   "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
					  "WHERE c.relkind IN ('r', 'v', 'S')\n");

	/*
	 * Unless a schema pattern is specified, we suppress system and temp
	 * tables, since they normally aren't very interesting from a permissions
	 * point of view.  You can see 'em by explicit request though, eg with \z
	 * pg_catalog.*
	 */
	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "c.relname", NULL,
			"n.nspname !~ '^pg_' AND pg_catalog.pg_table_is_visible(c.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	if (!res)
	{
		termPQExpBuffer(&buf);
		return false;
	}

	myopt.nullPrint = NULL;

    if (strcmp(get_line_style(&pset.popt.topt)->name,"old-ascii") == 0)
	{
	    printfPQExpBuffer(&buf, _("Access privileges for database \"%s\""), PQdb(pset.db));
	}
	else
	{
		/* Current pgsql does it this way, but I've disabled this and am doing it the old way above to avoid breaking regression tests */
	    printfPQExpBuffer(&buf, _("Access privileges"));
	}

	myopt.title = buf.data;
	myopt.translate_header = true;
	myopt.translate_columns = translate_columns;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	termPQExpBuffer(&buf);
	PQclear(res);
	return true;
}


/*
 * \ddp
 *
 * List DefaultACLs.  The pattern can match either schema or role name.
 */
bool
listDefaultACLs(const char *pattern)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;
	static const bool translate_columns[] = {false, false, true, false};

	if (pset.sversion < 90000)
	{
		fprintf(stderr, _("The server (version %d.%d) does not support altering default privileges.\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
			   "SELECT pg_catalog.pg_get_userbyid(d.defaclrole) AS \"%s\",\n"
					  "  n.nspname AS \"%s\",\n"
					  "  CASE d.defaclobjtype WHEN 'r' THEN '%s' WHEN 'S' THEN '%s' WHEN 'f' THEN '%s' END AS \"%s\",\n"
					  "  ",
					  gettext_noop("Owner"),
					  gettext_noop("Schema"),
					  gettext_noop("table"),
					  gettext_noop("sequence"),
					  gettext_noop("function"),
					  gettext_noop("Type"));

	printACLColumn(&buf, "d.defaclacl");

	appendPQExpBuffer(&buf, "\nFROM pg_catalog.pg_default_acl d\n"
					  "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = d.defaclnamespace\n");

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  NULL,
						  "n.nspname",
						  "pg_catalog.pg_get_userbyid(d.defaclrole)",
						  NULL);

	appendPQExpBuffer(&buf, "ORDER BY 1, 2, 3;");

	res = PSQLexec(buf.data, false);
	if (!res)
	{
		termPQExpBuffer(&buf);
		return false;
	}

	myopt.nullPrint = NULL;
	printfPQExpBuffer(&buf, _("Default access privileges"));
	myopt.title = buf.data;
	myopt.translate_header = true;
	myopt.translate_columns = translate_columns;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	termPQExpBuffer(&buf);
	PQclear(res);
	return true;
}


/*
 * Get object comments
 *
 * \dd [foo]
 *
 * Note: This only lists things that actually have a description. For complete
 * lists of things, there are other \d? commands.
 */
bool
objectDescription(const char *pattern, bool showSystem)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;
	static const bool translate_columns[] = {false, false, true, false};

	initPQExpBuffer(&buf);

	appendPQExpBuffer(&buf,
					  "SELECT DISTINCT tt.nspname AS \"%s\", tt.name AS \"%s\", tt.object AS \"%s\", d.description AS \"%s\"\n"
					  "FROM (\n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"),
					  gettext_noop("Object"),
					  gettext_noop("Description"));

	/* Aggregate descriptions */
	appendPQExpBuffer(&buf,
					  "  SELECT p.oid as oid, p.tableoid as tableoid,\n"
					  "  n.nspname as nspname,\n"
					  "  CAST(p.proname AS pg_catalog.text) as name,"
					  "  CAST('%s' AS pg_catalog.text) as object\n"
					  "  FROM pg_catalog.pg_proc p\n"
	 "       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n"
					  "  WHERE p.proisagg\n",
					  gettext_noop("aggregate"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "p.proname", NULL,
						  "pg_catalog.pg_function_is_visible(p.oid)");

	/* Function descriptions */
	appendPQExpBuffer(&buf,
					  "UNION ALL\n"
					  "  SELECT p.oid as oid, p.tableoid as tableoid,\n"
					  "  n.nspname as nspname,\n"
					  "  CAST(p.proname AS pg_catalog.text) as name,"
					  "  CAST('%s' AS pg_catalog.text) as object\n"
					  "  FROM pg_catalog.pg_proc p\n"
	 "       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n"
					  "  WHERE NOT p.proisagg\n",
					  gettext_noop("function"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "p.proname", NULL,
						  "pg_catalog.pg_function_is_visible(p.oid)");

	/* Operator descriptions (only if operator has its own comment) */
	appendPQExpBuffer(&buf,
					  "UNION ALL\n"
					  "  SELECT o.oid as oid, o.tableoid as tableoid,\n"
					  "  n.nspname as nspname,\n"
					  "  CAST(o.oprname AS pg_catalog.text) as name,"
					  "  CAST('%s' AS pg_catalog.text) as object\n"
					  "  FROM pg_catalog.pg_operator o\n"
	"       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace\n",
					  gettext_noop("operator"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "WHERE n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, !showSystem && !pattern, false,
						  "n.nspname", "o.oprname", NULL,
						  "pg_catalog.pg_operator_is_visible(o.oid)");

	/* Type description */
	appendPQExpBuffer(&buf,
					  "UNION ALL\n"
					  "  SELECT t.oid as oid, t.tableoid as tableoid,\n"
					  "  n.nspname as nspname,\n"
					  "  pg_catalog.format_type(t.oid, NULL) as name,"
					  "  CAST('%s' AS pg_catalog.text) as object\n"
					  "  FROM pg_catalog.pg_type t\n"
	"       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n",
					  gettext_noop("data type"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "WHERE n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, !showSystem && !pattern, false,
						  "n.nspname", "pg_catalog.format_type(t.oid, NULL)",
						  NULL,
						  "pg_catalog.pg_type_is_visible(t.oid)");

	/* Relation (tables, views, indexes, sequences, external) descriptions */
	appendPQExpBuffer(&buf,
					  "UNION ALL\n"
					  "  SELECT c.oid as oid, c.tableoid as tableoid,\n"
					  "  n.nspname as nspname,\n"
					  "  CAST(c.relname AS pg_catalog.text) as name,\n"
					  "  CAST(\n"
					  "    CASE c.relkind WHEN 'r' THEN '%s' WHEN 'v' THEN '%s' WHEN 'i' THEN '%s' WHEN 'S' THEN '%s' END"
					  "  AS pg_catalog.text) as object\n"
					  "  FROM pg_catalog.pg_class c\n"
	 "       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
					  "  WHERE c.relkind IN ('r', 'v', 'i', 'S')\n",
					  gettext_noop("table"),
					  gettext_noop("view"),
					  gettext_noop("index"),
					  gettext_noop("sequence"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "c.relname", NULL,
						  "pg_catalog.pg_table_is_visible(c.oid)");

	/* Rule description (ignore rules for views) */
	appendPQExpBuffer(&buf,
					  "UNION ALL\n"
					  "  SELECT r.oid as oid, r.tableoid as tableoid,\n"
					  "  n.nspname as nspname,\n"
					  "  CAST(r.rulename AS pg_catalog.text) as name,"
					  "  CAST('%s' AS pg_catalog.text) as object\n"
					  "  FROM pg_catalog.pg_rewrite r\n"
				  "       JOIN pg_catalog.pg_class c ON c.oid = r.ev_class\n"
	 "       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
					  "  WHERE r.rulename != '_RETURN'\n",
					  gettext_noop("rule"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	/* XXX not sure what to do about visibility rule here? */
	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "r.rulename", NULL,
						  "pg_catalog.pg_table_is_visible(c.oid)");

	/* Trigger description */
	appendPQExpBuffer(&buf,
					  "UNION ALL\n"
					  "  SELECT t.oid as oid, t.tableoid as tableoid,\n"
					  "  n.nspname as nspname,\n"
					  "  CAST(t.tgname AS pg_catalog.text) as name,"
					  "  CAST('%s' AS pg_catalog.text) as object\n"
					  "  FROM pg_catalog.pg_trigger t\n"
				   "       JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid\n"
	"       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n",
					  gettext_noop("trigger"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "WHERE n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	/* XXX not sure what to do about visibility rule here? */
	processSQLNamePattern(pset.db, &buf, pattern, !showSystem && !pattern, false,
						  "n.nspname", "t.tgname", NULL,
						  "pg_catalog.pg_table_is_visible(c.oid)");

	appendPQExpBuffer(&buf,
					  ") AS tt\n"
					  "  JOIN pg_catalog.pg_description d ON (tt.oid = d.objoid AND tt.tableoid = d.classoid AND d.objsubid = 0)\n");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2, 3;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("Object descriptions");
	myopt.translate_header = true;
	myopt.translate_columns = translate_columns;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}


/*
 * describeTableDetails (for \d)
 *
 * This routine finds the tables to be displayed, and calls
 * describeOneTableDetails for each one.
 *
 * verbose: if true, this is \d+
 */
bool
describeTableDetails(const char *pattern, bool verbose, bool showSystem)
{
	PQExpBufferData buf;
	PGresult   *res;
	int			i;

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT c.oid,\n"
					  "  n.nspname,\n"
					  "  c.relname\n"
					  "FROM pg_catalog.pg_class c\n"
	 "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n");

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "WHERE n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, !showSystem && !pattern, false,
						  "n.nspname", "c.relname", NULL,
						  "pg_catalog.pg_table_is_visible(c.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 2, 3;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	if (PQntuples(res) == 0)
	{
		if (!pset.quiet)
			fprintf(stderr, _("Did not find any relation named \"%s\".\n"),
					pattern);
		PQclear(res);
		return false;
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		const char *oid;
		const char *nspname;
		const char *relname;

		oid = PQgetvalue(res, i, 0);
		nspname = PQgetvalue(res, i, 1);
		relname = PQgetvalue(res, i, 2);

		if (!describeOneTableDetails(nspname, relname, oid, verbose))
		{
			PQclear(res);
			return false;
		}
		if (cancel_pressed)
		{
			PQclear(res);
			return false;
		}
	}

	PQclear(res);
	return true;
}

/*
 * describeOneTableDetails (for \d)
 *
 * Unfortunately, the information presented here is so complicated that it
 * cannot be done in a single query. So we have to assemble the printed table
 * by hand and pass it to the underlying printTable() function.
 */
static bool
describeOneTableDetails(const char *schemaname,
						const char *relationname,
						const char *oid,
						bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res = NULL;
	printTableOpt myopt = pset.popt.topt;
	printTableContent cont;
	bool		printTableInitialized = false;
	int			i;
	char	   *view_def = NULL;
	char	   *headers[9];
	char	  **seq_values = NULL;
	char	  **modifiers = NULL;
	char	  **ptr;
	PQExpBufferData title;
	PQExpBufferData tmpbuf;
	int			cols = 0;
	int			numrows = 0;
	bool isGE42 = isGPDB4200OrLater();
	struct
	{
		int16		checks;
		char		relkind;
		char		relstorage;
		bool		hasindex;
		bool		hasrules;
		bool		hastriggers;
		bool		hasoids;
		Oid			tablespace;
		char	   *reloptions;
		char	   *reloftype;
		char	   *compressionType;
		char	   *compressionLevel;
		char	   *blockSize;
		char	   *pageSize;
		char	   *rowgroupSize;
		char	   *checksum;
	}			tableinfo;
	bool		show_modifiers = false;
	bool		retval;

	tableinfo.compressionType  = NULL;
	tableinfo.compressionLevel = NULL;
	tableinfo.blockSize        = NULL;
	tableinfo.pageSize		   = NULL;
	tableinfo.rowgroupSize	   = NULL;
	tableinfo.checksum         = NULL;

	retval = false;

	/* This output looks confusing in expanded mode. */
	myopt.expanded = false;

	initPQExpBuffer(&buf);
	initPQExpBuffer(&title);
	initPQExpBuffer(&tmpbuf);

	/* Get general table info */
	if (pset.sversion >= 90000)
	{
		printfPQExpBuffer(&buf,
			  "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, "
						  "c.relhastriggers, c.relhasoids, "
						  "%s, c.reltablespace, "
						  "CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END\n"
						  "FROM pg_catalog.pg_class c\n "
		   "LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)\n"
						  "WHERE c.oid = '%s'\n",
						  (verbose ?
						   "pg_catalog.array_to_string(c.reloptions || "
						   "array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x), ', ')\n"
						   : "''"),
						  oid);
	}
	else if (pset.sversion >= 80400)
	{
		printfPQExpBuffer(&buf,
			  "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, "
						  "c.relhastriggers, c.relhasoids, "
						  "%s, c.reltablespace\n"
						  "FROM pg_catalog.pg_class c\n "
		   "LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)\n"
						  "WHERE c.oid = '%s'\n",
						  (verbose ?
						   "pg_catalog.array_to_string(c.reloptions || "
						   "array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x), ', ')\n"
						   : "''"),
						  oid);
	}
	else if (pset.sversion >= 80200)
	{
		printfPQExpBuffer(&buf,
					  "SELECT relchecks, relkind, relhasindex, relhasrules, "
						  "reltriggers <> 0, relhasoids, "
						  "%s, reltablespace, %s\n"
						  "FROM pg_catalog.pg_class WHERE oid = '%s'",
						  (verbose ?
					 "pg_catalog.array_to_string(reloptions, E', ')" : "''"),
					 /* GPDB Only:  relstorage  */
					 	  (isGPDB() ?
					 		"relstorage" : "'h'"),
						  oid);
	}
	else if (pset.sversion >= 80000)
	{
		printfPQExpBuffer(&buf,
					  "SELECT relchecks, relkind, relhasindex, relhasrules, "
						  "reltriggers <> 0, relhasoids, "
						  "'', reltablespace\n"
						  "FROM pg_catalog.pg_class WHERE oid = '%s'",
						  oid);
	}
	else
	{
		printfPQExpBuffer(&buf,
					  "SELECT relchecks, relkind, relhasindex, relhasrules, "
						  "reltriggers <> 0, relhasoids, "
						  "'', ''\n"
						  "FROM pg_catalog.pg_class WHERE oid = '%s'",
						  oid);
	}

	res = PSQLexec(buf.data, false);
	if (!res)
		goto error_return;

	/* Did we get anything? */
	if (PQntuples(res) == 0)
	{
		if (!pset.quiet)
			fprintf(stderr, _("Did not find any relation with OID %s.\n"),
					oid);
		goto error_return;
	}

	tableinfo.checks = atoi(PQgetvalue(res, 0, 0));
	tableinfo.relkind = *(PQgetvalue(res, 0, 1));
	tableinfo.hasindex = strcmp(PQgetvalue(res, 0, 2), "t") == 0;
	tableinfo.hasrules = strcmp(PQgetvalue(res, 0, 3), "t") == 0;
	tableinfo.hastriggers = strcmp(PQgetvalue(res, 0, 4), "t") == 0;
	tableinfo.hasoids = strcmp(PQgetvalue(res, 0, 5), "t") == 0;
	tableinfo.reloptions = (pset.sversion >= 80200) ?
		strdup(PQgetvalue(res, 0, 6)) : 0;
	tableinfo.tablespace = (pset.sversion >= 80000) ?
		atooid(PQgetvalue(res, 0, 7)) : 0;
	tableinfo.reloftype = (pset.sversion >= 90000 && strcmp(PQgetvalue(res, 0, 8), "") != 0) ?
		strdup(PQgetvalue(res, 0, 8)) : 0;
	/* GPDB Only:  relstorage  */
	tableinfo.relstorage = (isGPDB()) ?
		*(PQgetvalue(res, 0, 8)) : 'h';
	PQclear(res);
	res = NULL;

	/*
	 * If it's a sequence, fetch its values and store into an array that will
	 * be used later.
	 */
	if (tableinfo.relkind == 'S')
	{
		printfPQExpBuffer(&buf, "SELECT * FROM %s", fmtId(schemaname));
		/* must be separate because fmtId isn't reentrant */
		appendPQExpBuffer(&buf, ".%s", fmtId(relationname));

		res = PSQLexec(buf.data, false);
		if (!res)
			goto error_return;

		seq_values = pg_malloc((PQnfields(res) + 1) * sizeof(*seq_values));

		for (i = 0; i < PQnfields(res); i++)
			seq_values[i] = pg_strdup(PQgetvalue(res, 0, i));
		seq_values[i] = NULL;

		PQclear(res);
		res = NULL;
	}

	if (tableinfo.relstorage == 'a' || tableinfo.relstorage == 'c')
	{
		PGresult *result = NULL;
		/* Get Append Only information
		 * always have 4 bits of info: blocksize, compresstype, compresslevel and checksum
		 */
		printfPQExpBuffer(&buf,
				"SELECT a.compresstype, a.compresslevel, a.blocksize, a.checksum\n"
					"FROM pg_catalog.pg_appendonly a, pg_catalog.pg_class c\n"
					"WHERE c.oid = a.relid AND c.oid = '%s'", oid);

		result = PSQLexec(buf.data, false);
		if (!result)
			goto error_return;

		if (PQgetisnull(result, 0, 0))
		{
			tableinfo.compressionType = pg_malloc(sizeof("None") + 1);
			strcpy(tableinfo.compressionType, "None");
		} else
			tableinfo.compressionType = pg_strdup(PQgetvalue(result, 0, 0));
		tableinfo.compressionLevel = pg_strdup(PQgetvalue(result, 0, 1));
		tableinfo.blockSize = pg_strdup(PQgetvalue(result, 0, 2));
		tableinfo.checksum = pg_strdup(PQgetvalue(result, 0, 3));
		PQclear(res);
		res = NULL;
	}

	if (tableinfo.relstorage == 'p')
	{
		PGresult *result = NULL;
		/* Get Append Only information
		 * always have 4 bits of info: blocksize, compresstype, compresslevel and checksum
		 */
		printfPQExpBuffer(&buf,
				"SELECT p.compresstype, p.compresslevel, p.pagesize, p.blocksize, p.checksum\n"
					"FROM pg_catalog.pg_appendonly p, pg_catalog.pg_class c\n"
					"WHERE c.oid = p.relid AND c.oid = '%s'", oid);

		result = PSQLexec(buf.data, false);
		if (!result)
			goto error_return;

		if (PQgetisnull(result, 0, 0))
		{
			tableinfo.compressionType = pg_malloc(sizeof("None") + 1);
			strcpy(tableinfo.compressionType, "None");
		} else
			tableinfo.compressionType = pg_strdup(PQgetvalue(result, 0, 0));
		tableinfo.compressionLevel = pg_strdup(PQgetvalue(result, 0, 1));
		tableinfo.pageSize = pg_strdup(PQgetvalue(result, 0, 2));
		tableinfo.rowgroupSize = pg_strdup(PQgetvalue(result, 0, 3));
		tableinfo.checksum = pg_strdup(PQgetvalue(result, 0, 4));
		PQclear(res);
		res = NULL;
	}

	/* Get column info */
	printfPQExpBuffer(&buf, "SELECT a.attname,");
	appendPQExpBuffer(&buf, "\n  pg_catalog.format_type(a.atttypid, a.atttypmod),"
					  "\n  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)"
					  "\n   FROM pg_catalog.pg_attrdef d"
					  "\n   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),"
					  "\n  a.attnotnull, a.attnum");
	if (tableinfo.relkind == 'i')
		appendPQExpBuffer(&buf, ",\n  pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE) AS indexdef");
	if (verbose)
	{
		appendPQExpBuffer(&buf, ",\n  a.attstorage ");
		if (tableinfo.relstorage == 'c')
			if (isGE42 == true)
		     appendPQExpBuffer(&buf, ",\n pg_catalog.array_to_string(e.attoptions, ',')");
		appendPQExpBuffer(&buf, ",\n  pg_catalog.col_description(a.attrelid, a.attnum)");
	}
	appendPQExpBuffer(&buf, "\nFROM pg_catalog.pg_attribute a ");
	if (isGE42 == true)
	{
	  appendPQExpBuffer(&buf, "\nLEFT OUTER JOIN pg_catalog.pg_attribute_encoding e");
	  appendPQExpBuffer(&buf, "\nON   e.attrelid = a .attrelid AND e.attnum = a.attnum");
	}
	appendPQExpBuffer(&buf, "\nWHERE a.attrelid = '%s' AND a.attnum > 0 AND NOT a.attisdropped", oid);
	appendPQExpBuffer(&buf, "\nORDER BY a.attnum");

	res = PSQLexec(buf.data, false);
	if (!res)
		goto error_return;
	numrows = PQntuples(res);

	/* Make title */
	switch (tableinfo.relkind)
	{
		case 'r':
			if(tableinfo.relstorage == 'a')
				printfPQExpBuffer(&title, _("Append-Only Table \"%s.%s\""),
								  schemaname, relationname);
			else if(tableinfo.relstorage == 'c')
				printfPQExpBuffer(&title, _("Append-Only Columnar Table \"%s.%s\""),
								  schemaname, relationname);
			else if(tableinfo.relstorage == 'p')
				printfPQExpBuffer(&title, _("Parquet Table \"%s.%s\""),
								schemaname, relationname);
			else if(tableinfo.relstorage == 'x')
				printfPQExpBuffer(&title, _("External table \"%s.%s\""),
								  schemaname, relationname);
			else if(tableinfo.relstorage == 'f')
				printfPQExpBuffer(&title, _("Foreign table \"%s.%s\""),
								  schemaname, relationname);
			else
				printfPQExpBuffer(&title, _("Table \"%s.%s\""),
								  schemaname, relationname);
			break;
		case 'v':
			printfPQExpBuffer(&title, _("View \"%s.%s\""),
							  schemaname, relationname);
			break;
		case 'S':
			printfPQExpBuffer(&title, _("Sequence \"%s.%s\""),
							  schemaname, relationname);
			break;
		case 'i':
			printfPQExpBuffer(&title, _("Index \"%s.%s\""),
							  schemaname, relationname);
			break;
		case 's':
			/* not used as of 8.2, but keep it for backwards compatibility */
			printfPQExpBuffer(&title, _("Special relation \"%s.%s\""),
							  schemaname, relationname);
			break;
		case 't':
			printfPQExpBuffer(&title, _("TOAST table \"%s.%s\""),
							  schemaname, relationname);
			break;
		case 'c':
			printfPQExpBuffer(&title, _("Composite type \"%s.%s\""),
							  schemaname, relationname);
			break;
		default:
			printfPQExpBuffer(&title, _("?%c? \"%s.%s\""),
							  tableinfo.relkind, schemaname, relationname);
			break;
	}

	/* Set the number of columns, and their names */
	headers[0] = gettext_noop("Column");
	headers[1] = gettext_noop("Type");
	cols = 2;

	if (tableinfo.relkind == 'r' || tableinfo.relkind == 'v')
	{
		show_modifiers = true;
		headers[cols++] = gettext_noop("Modifiers");
		modifiers = pg_malloc_zero((numrows + 1) * sizeof(*modifiers));
	}

	if (tableinfo.relkind == 'S')
		headers[cols++] = gettext_noop("Value");

	if (tableinfo.relkind == 'i')
		headers[cols++] = gettext_noop("Definition");

	if (verbose)
	{
		headers[cols++] = gettext_noop("Storage");
		if(tableinfo.relstorage == 'c')
		{
		  headers[cols++] = gettext_noop("Compression Type");
		  headers[cols++] = gettext_noop("Compression Level");
		  headers[cols++] = gettext_noop("Block Size");
		}
		headers[cols++] = gettext_noop("Description");
	}

	printTableInit(&cont, &myopt, title.data, cols, numrows);
	printTableInitialized = true;

	for (i = 0; i < cols; i++)
		printTableAddHeader(&cont, headers[i], true, 'l');

	/* Check if table is a view */
	if (tableinfo.relkind == 'v' /* && verbose  ***  GPDB change:  Do this even if not verbose, for 8.2 compatibility */)
	{
		PGresult   *result;

		printfPQExpBuffer(&buf,
			  "SELECT pg_catalog.pg_get_viewdef('%s'::pg_catalog.oid, true)",
						  oid);
		result = PSQLexec(buf.data, false);
		if (!result)
			goto error_return;

		if (PQntuples(result) > 0)
			view_def = pg_strdup(PQgetvalue(result, 0, 0));

		PQclear(result);
	}

	/* Generate table cells to be printed */
	for (i = 0; i < numrows; i++)
	{
		/* Column */
		printTableAddCell(&cont, PQgetvalue(res, i, 0), false, false);

		/* Type */
		printTableAddCell(&cont, PQgetvalue(res, i, 1), false, false);

		/* Modifiers: not null and default */
		if (show_modifiers)
		{
			resetPQExpBuffer(&tmpbuf);
			if (strcmp(PQgetvalue(res, i, 3), "t") == 0)
				appendPQExpBufferStr(&tmpbuf, _("not null"));

			/* handle "default" here */
			/* (note: above we cut off the 'default' string at 128) */
			if (strlen(PQgetvalue(res, i, 2)) != 0)
			{
				if (tmpbuf.len > 0)
					appendPQExpBufferStr(&tmpbuf, " ");
				/* translator: default values of column definitions */
				appendPQExpBuffer(&tmpbuf, _("default %s"),
								  PQgetvalue(res, i, 2));
			}

			modifiers[i] = pg_strdup(tmpbuf.data);
			printTableAddCell(&cont, modifiers[i], false, false);
		}

		/* Value: for sequences only */
		if (tableinfo.relkind == 'S')
			printTableAddCell(&cont, seq_values[i], false, false);

		/* Expression for index column */
		if (tableinfo.relkind == 'i')
			printTableAddCell(&cont, PQgetvalue(res, i, 5), false, false);

		/* Storage and Description */
		if (verbose)
		{
			int			firstvcol = (tableinfo.relkind == 'i' ? 6 : 5);
			int			firstvcol_offset = 0;
			char	   *storage = PQgetvalue(res, i, firstvcol);

			/* Storage */
			/* these strings are literal in our syntax, so not translated. */
			printTableAddCell(&cont, (storage[0] == 'p' ? "plain" :
									  (storage[0] == 'm' ? "main" :
									   (storage[0] == 'x' ? "extended" :
										(storage[0] == 'e' ? "external" :
										 "???")))),
							  false, false);
			firstvcol_offset = firstvcol_offset + 1;

			if (tableinfo.relstorage == 'c')
			{

				/* The compression type, compression level, and block size are all in the next column.
				 * attributeOptions is a text array of key=value pairs retrieved as a string from the catalog.
				 * Each key=value pair is separated by a ",".
				 *
				 * If the table was created pre-4.2, then it will not have entries in the new pg_attribute_storage table.
				 * If there are no entries, we go to the pre-4.1 values stored in the pg_appendonly table.
				 */
				char *attributeOptions;
				if (isGE42 == true)
				{
				   attributeOptions = PQgetvalue(res, i, firstvcol + firstvcol_offset); /* pg_catalog.pg_attribute_storage(attoptions) */
				   firstvcol_offset = firstvcol_offset + 1;
				}
				else
					 attributeOptions = pg_malloc_zero(1);  /* Make an empty options string so the reset of the code works correctly. */
				char *key = strtok(attributeOptions, ",=");
				char *value = NULL;
				char *compressionType = NULL;
				char *compressionLevel = NULL;
				char *blockSize = NULL;

				while (key != NULL)
				{
					value = strtok(NULL, ",=");
					if (strcmp(key, "compresstype") == 0)
						compressionType = value;
					else if (strcmp(key, "compresslevel") == 0)
						compressionLevel = value;
					else if (strcmp(key, "blocksize") == 0)
						blockSize = value;
					key = strtok(NULL, ",=");
				}

				/* Compression Type */
				if (compressionType == NULL)
					printTableAddCell(&cont, tableinfo.compressionType, false, false);
				else
					printTableAddCell(&cont, compressionType, false, false);

				/* Compression Level */
				if (compressionLevel == NULL)
					printTableAddCell(&cont, tableinfo.compressionLevel, false, false);
				else
					printTableAddCell(&cont, compressionLevel, false, false);

				/* Block Size */
				if (blockSize == NULL)
					printTableAddCell(&cont, tableinfo.blockSize, false, false);
				else
					printTableAddCell(&cont, blockSize, false, false);
			}

			/* Description */
			printTableAddCell(&cont, PQgetvalue(res, i, firstvcol + firstvcol_offset),
							  false, false);
		}
	}

	/* Make footers */
	if (tableinfo.relkind == 'i')
	{
		/* Footer information about an index */
		PGresult   *result;

		printfPQExpBuffer(&buf,
				 "SELECT i.indisunique, i.indisprimary, i.indisclustered, ");
		if (pset.sversion >= 80200)
			appendPQExpBuffer(&buf, "i.indisvalid,\n");
		else
			appendPQExpBuffer(&buf, "true AS indisvalid,\n");
		if (pset.sversion >= 90000)
			appendPQExpBuffer(&buf,
							  "  (NOT i.indimmediate) AND "
							"EXISTS (SELECT 1 FROM pg_catalog.pg_constraint "
							  "WHERE conrelid = i.indrelid AND "
							  "conindid = i.indexrelid AND "
							  "contype IN ('p','u','x') AND "
							  "condeferrable) AS condeferrable,\n"
							  "  (NOT i.indimmediate) AND "
							"EXISTS (SELECT 1 FROM pg_catalog.pg_constraint "
							  "WHERE conrelid = i.indrelid AND "
							  "conindid = i.indexrelid AND "
							  "contype IN ('p','u','x') AND "
							  "condeferred) AS condeferred,\n");
		else
			appendPQExpBuffer(&buf,
						"  false AS condeferrable, false AS condeferred,\n");
		appendPQExpBuffer(&buf, "  a.amname, c2.relname, "
					  "pg_catalog.pg_get_expr(i.indpred, i.indrelid, true)\n"
						  "FROM pg_catalog.pg_index i, pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_am a\n"
		  "WHERE i.indexrelid = c.oid AND c.oid = '%s' AND c.relam = a.oid\n"
						  "AND i.indrelid = c2.oid",
						  oid);

		result = PSQLexec(buf.data, false);
		if (!result)
			goto error_return;
		else if (PQntuples(result) != 1)
		{
			PQclear(result);
			goto error_return;
		}
		else
		{
			char	   *indisunique = PQgetvalue(result, 0, 0);
			char	   *indisprimary = PQgetvalue(result, 0, 1);
			char	   *indisclustered = PQgetvalue(result, 0, 2);
			char	   *indisvalid = PQgetvalue(result, 0, 3);
			char	   *deferrable = PQgetvalue(result, 0, 4);
			char	   *deferred = PQgetvalue(result, 0, 5);
			char	   *indamname = PQgetvalue(result, 0, 6);
			char	   *indtable = PQgetvalue(result, 0, 7);
			char	   *indpred = PQgetvalue(result, 0, 8);

			if (strcmp(indisprimary, "t") == 0)
				printfPQExpBuffer(&tmpbuf, _("primary key, "));
			else if (strcmp(indisunique, "t") == 0)
				printfPQExpBuffer(&tmpbuf, _("unique, "));
			else
				resetPQExpBuffer(&tmpbuf);
			appendPQExpBuffer(&tmpbuf, "%s, ", indamname);

			/* we assume here that index and table are in same schema */
			appendPQExpBuffer(&tmpbuf, _("for table \"%s.%s\""),
							  schemaname, indtable);

			if (strlen(indpred))
				appendPQExpBuffer(&tmpbuf, _(", predicate (%s)"), indpred);

			if (strcmp(indisclustered, "t") == 0)
				appendPQExpBuffer(&tmpbuf, _(", clustered"));

			if (strcmp(indisvalid, "t") != 0)
				appendPQExpBuffer(&tmpbuf, _(", invalid"));

			if (strcmp(deferrable, "t") == 0)
				appendPQExpBuffer(&tmpbuf, _(", deferrable"));

			if (strcmp(deferred, "t") == 0)
				appendPQExpBuffer(&tmpbuf, _(", initially deferred"));

			printTableAddFooter(&cont, tmpbuf.data);
			add_tablespace_footer(&cont, tableinfo.relkind,
								  tableinfo.tablespace, true);
		}

		PQclear(result);
	}
	else if (tableinfo.relstorage == 'x')
	{
		/* Footer information about an external table */
		PGresult   *result;

		printfPQExpBuffer(&buf,
						  "SELECT x.location, x.fmttype, x.fmtopts, x.command, "
						         "x.rejectlimit, x.rejectlimittype, x.writable, "
						         "(SELECT relname "
						          "FROM pg_class "
								  "WHERE Oid=x.fmterrtbl) AS errtblname, "
								  "pg_catalog.pg_encoding_to_char(x.encoding) "
						  "FROM pg_catalog.pg_exttable x, pg_catalog.pg_class c "
						  "WHERE x.reloid = c.oid AND c.oid = '%s'\n", oid);

		result = PSQLexec(buf.data, false);
		if (!result)
			goto error_return;
		else if (PQntuples(result) != 1)
		{
			PQclear(result);
			goto error_return;
		}
		else
		{
			char	   *location = PQgetvalue(result, 0, 0);
			char	   *fmttype = PQgetvalue(result, 0, 1);
			char	   *fmtopts = PQgetvalue(result, 0, 2);
			char	   *command = PQgetvalue(result, 0, 3);
			char	   *rejlim =  PQgetvalue(result, 0, 4);
			char	   *rejlimtype = PQgetvalue(result, 0, 5);
			char	   *writable = PQgetvalue(result, 0, 6);
			char	   *errtblname = PQgetvalue(result, 0, 7);
			char	   *extencoding = PQgetvalue(result, 0, 8);
			char       *format;

			/* Writable/Readable */
			printfPQExpBuffer(&tmpbuf, _("Type: %s"), writable[0] == 't' ? "writable" : "readable");
			printTableAddFooter(&cont, tmpbuf.data);

			/* encoding */
			printfPQExpBuffer(&tmpbuf, _("Encoding: %s"), extencoding);
			printTableAddFooter(&cont, tmpbuf.data);

			/* format type */
			switch ( fmttype[0] )
			{
				case 't':
				{
					format = "text";
				}
				break;
				case 'c':
				{
					format = "csv";
				}
				break;
				case 'b':
				{
					format = "custom";
				}
				break;
				default:
				{
					format = "";
					fprintf(stderr, _("Unknown fmttype value: %c\n"), fmttype[0]);
				}
				break;
			};
			printfPQExpBuffer(&tmpbuf, _("Format type: %s"), format);
			printTableAddFooter(&cont, tmpbuf.data);

			/* format options */
			printfPQExpBuffer(&tmpbuf, _("Format options: %s"), fmtopts);
			printTableAddFooter(&cont, tmpbuf.data);

			if(command && strlen(command) > 0)
			{
				/* EXECUTE type table - show command and command location */

				printfPQExpBuffer(&tmpbuf, _("Command: %s"), command);
				printTableAddFooter(&cont, tmpbuf.data);

				location[strlen(location) - 1] = '\0'; /* don't print the '}' character */
				location++; /* don't print the '{' character */

				if(strncmp(location, "HOST:", strlen("HOST:")) == 0)
					printfPQExpBuffer(&tmpbuf, _("Execute on: host '%s'"), location + strlen("HOST:"));
				else if(strncmp(location, "PER_HOST", strlen("PER_HOST")) == 0)
					printfPQExpBuffer(&tmpbuf, _("Execute on: one segment per host"));
				else if(strncmp(location, "MASTER_ONLY", strlen("MASTER_ONLY")) == 0)
					printfPQExpBuffer(&tmpbuf, _("Execute on: master segment"));
				else if(strncmp(location, "SEGMENT_ID:", strlen("SEGMENT_ID:")) == 0)
					printfPQExpBuffer(&tmpbuf, _("Execute on: segment %s"), location + strlen("SEGMENT_ID:"));
				else if(strncmp(location, "TOTAL_SEGS:", strlen("TOTAL_SEGS:")) == 0)
					printfPQExpBuffer(&tmpbuf, _("Execute on: %s random segments"), location + strlen("TOTAL_SEGS:"));
				else if(strncmp(location, "ALL_SEGMENTS", strlen("ALL_SEGMENTS")) == 0)
					printfPQExpBuffer(&tmpbuf, _("Execute on: all segments"));
				else
					printfPQExpBuffer(&tmpbuf, _("Execute on: ERROR: invalid catalog entry (describe.c)"));

				printTableAddFooter(&cont, tmpbuf.data);

			}
			else
			{
				/* LOCATION type table - show external location */

				location[strlen(location) - 1] = '\0'; /* don't print the '}' character */
				location++; /* don't print the '{' character */
				printfPQExpBuffer(&tmpbuf, _("External location: %s"), location);
				printTableAddFooter(&cont, tmpbuf.data);
			}

			/* Single row error handling */
			if(rejlim && strlen(rejlim) > 0)
			{
				/* reject limit and type */
				printfPQExpBuffer(&tmpbuf, _("Segment reject limit: %s %s"),
								  rejlim,
								  (rejlimtype[0] == 'p' ? "percent" : "rows"));
				printTableAddFooter(&cont, tmpbuf.data);

				if(errtblname && strlen(errtblname) > 0)
				{
					printfPQExpBuffer(&tmpbuf, _("Error table: %s"), errtblname);
					printTableAddFooter(&cont, tmpbuf.data);
				}
			}

			if(writable[0] == 't')
				if(add_distributed_by_footer(oid, &tmpbuf, buf))
					goto error_return;

			add_tablespace_footer(&cont, tableinfo.relkind, tableinfo.tablespace, true);
		}

		PQclear(result);

	}
	else if (view_def)
	{
		PGresult   *result = NULL;

		/* Footer information about a view */
		printTableAddFooter(&cont, _("View definition:"));
		printTableAddFooter(&cont, view_def);

		/* print rules */
		if (tableinfo.hasrules)
		{
			printfPQExpBuffer(&buf,
							  "SELECT r.rulename, trim(trailing ';' from pg_catalog.pg_get_ruledef(r.oid, true))\n"
							  "FROM pg_catalog.pg_rewrite r\n"
			"WHERE r.ev_class = '%s' AND r.rulename != '_RETURN' ORDER BY 1",
							  oid);
			result = PSQLexec(buf.data, false);
			if (!result)
				goto error_return;

			if (PQntuples(result) > 0)
			{
				printTableAddFooter(&cont, _("Rules:"));
				for (i = 0; i < PQntuples(result); i++)
				{
					const char *ruledef;

					/* Everything after "CREATE RULE" is echoed verbatim */
					ruledef = PQgetvalue(result, i, 1);
					ruledef += 12;

					printfPQExpBuffer(&buf, " %s", ruledef);
					printTableAddFooter(&cont, buf.data);
				}
			}
			PQclear(result);
		}
	}
	else if (tableinfo.relkind == 'r')
	{
		/* Footer information about a table */
		PGresult   *result = NULL;
		int			tuples = 0;

		/* print append only table information */
		if (tableinfo.relstorage == 'a' || tableinfo.relstorage == 'c' || tableinfo.relstorage == 'p')
		{
		  if (tableinfo.relstorage != 'c')
			{
				printfPQExpBuffer(&buf, _("Compression Type: %s"), tableinfo.compressionType);
				printTableAddFooter(&cont, buf.data);
				printfPQExpBuffer(&buf, _("Compression Level: %s"), tableinfo.compressionLevel);
				printTableAddFooter(&cont, buf.data);
				if(tableinfo.relstorage == 'a')
				{
					printfPQExpBuffer(&buf, _("Block Size: %s"), tableinfo.blockSize);
					printTableAddFooter(&cont, buf.data);
				}
				else
				{
					printfPQExpBuffer(&buf, _("Page Size: %s"), tableinfo.pageSize);
					printTableAddFooter(&cont, buf.data);
					printfPQExpBuffer(&buf, _("RowGroup Size: %s"), tableinfo.rowgroupSize);
					printTableAddFooter(&cont, buf.data);
				}

			}
			printfPQExpBuffer(&buf, _("Checksum: %s"), tableinfo.checksum);
			printTableAddFooter(&cont, buf.data);
		}


		/* print foreign table information */
        if (tableinfo.relstorage == 'f')
		{
			/* count and get Foreign table footers
			 * always have 1 footer: server name
			 */
			printfPQExpBuffer(&buf,
							  "SELECT s.srvname\n"
							  "FROM pg_catalog.pg_foreign_table f, pg_catalog.pg_class c, pg_catalog.pg_foreign_server s\n"
							  "WHERE c.oid = f.reloid AND f.server = s.oid AND c.oid = '%s'", oid);

			result = PSQLexec(buf.data, false);
			if (!result)
				goto error_return;

			printfPQExpBuffer(&buf, _("Foreign Server: %s"), PQgetvalue(result, 0, 0));
			printTableAddFooter(&cont, buf.data);
		}

        /* print indexes */
		if (tableinfo.hasindex)
		{
			printfPQExpBuffer(&buf,
							  "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, ");
			if (pset.sversion >= 80200)
				appendPQExpBuffer(&buf, "i.indisvalid, ");
			else
				appendPQExpBuffer(&buf, "true as indisvalid, ");
			appendPQExpBuffer(&buf, "pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),\n  ");
			if (pset.sversion >= 90000)
				appendPQExpBuffer(&buf,
						   "pg_catalog.pg_get_constraintdef(con.oid, true), "
								  "contype, condeferrable, condeferred");
			else
				appendPQExpBuffer(&buf,
								  "null AS constraintdef, null AS contype, "
							 "false AS condeferrable, false AS condeferred");
			if (pset.sversion >= 80000)
				appendPQExpBuffer(&buf, ", c2.reltablespace");
			appendPQExpBuffer(&buf,
							  "\nFROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i\n");
			if (pset.sversion >= 90000)
				appendPQExpBuffer(&buf,
								  "  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))\n");
			appendPQExpBuffer(&buf,
							  "WHERE c.oid = '%s' AND c.oid = i.indrelid AND i.indexrelid = c2.oid\n"
			  "ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname",
							  oid);
			result = PSQLexec(buf.data, false);
			if (!result)
				goto error_return;
			else
				tuples = PQntuples(result);

			if (tuples > 0)
			{
				printTableAddFooter(&cont, _("Indexes:"));
				for (i = 0; i < tuples; i++)
				{
					/* untranslated index name */
					printfPQExpBuffer(&buf, "    \"%s\"",
									  PQgetvalue(result, i, 0));

					/* If exclusion constraint, print the constraintdef */
					if (strcmp(PQgetvalue(result, i, 7), "x") == 0)
					{
						appendPQExpBuffer(&buf, " %s",
										  PQgetvalue(result, i, 6));
					}
					else
					{
						const char *indexdef;
						const char *usingpos;

						/* Label as primary key or unique (but not both) */
						if (strcmp(PQgetvalue(result, i, 1), "t") == 0)
							appendPQExpBuffer(&buf, " PRIMARY KEY,");
						else if (strcmp(PQgetvalue(result, i, 2), "t") == 0)
							appendPQExpBuffer(&buf, " UNIQUE,");

						/* Everything after "USING" is echoed verbatim */
						indexdef = PQgetvalue(result, i, 5);
						usingpos = strstr(indexdef, " USING ");
						if (usingpos)
							indexdef = usingpos + 7;
						appendPQExpBuffer(&buf, " %s", indexdef);

						/* Need these for deferrable PK/UNIQUE indexes */
						if (strcmp(PQgetvalue(result, i, 8), "t") == 0)
							appendPQExpBuffer(&buf, " DEFERRABLE");

						if (strcmp(PQgetvalue(result, i, 9), "t") == 0)
							appendPQExpBuffer(&buf, " INITIALLY DEFERRED");
					}

					/* Add these for all cases */
					if (strcmp(PQgetvalue(result, i, 3), "t") == 0)
						appendPQExpBuffer(&buf, " CLUSTER");

					if (strcmp(PQgetvalue(result, i, 4), "t") != 0)
						appendPQExpBuffer(&buf, " INVALID");

					printTableAddFooter(&cont, buf.data);

					/* Print tablespace of the index on the same line */
					if (pset.sversion >= 80000)
						add_tablespace_footer(&cont, 'i',
										   atooid(PQgetvalue(result, i, 10)),
											  false);
				}
			}
			PQclear(result);
		}

		/* print table (and column) check constraints */
		if (tableinfo.checks)
		{
			printfPQExpBuffer(&buf,
							  "SELECT r.conname, "
							  "pg_catalog.pg_get_constraintdef(r.oid, true)\n"
							  "FROM pg_catalog.pg_constraint r\n"
				   "WHERE r.conrelid = '%s' AND r.contype = 'c'\nORDER BY 1",
							  oid);
			result = PSQLexec(buf.data, false);
			if (!result)
				goto error_return;
			else
				tuples = PQntuples(result);

			if (tuples > 0)
			{
				printTableAddFooter(&cont, _("Check constraints:"));
				for (i = 0; i < tuples; i++)
				{
					/* untranslated contraint name and def */
					printfPQExpBuffer(&buf, "    \"%s\" %s",
									  PQgetvalue(result, i, 0),
									  PQgetvalue(result, i, 1));

					printTableAddFooter(&cont, buf.data);
				}
			}
			PQclear(result);
		}

		/* print foreign-key constraints (there are none if no triggers) */
		if (tableinfo.hastriggers)
		{
			printfPQExpBuffer(&buf,
							  "SELECT conname,\n"
				 "  pg_catalog.pg_get_constraintdef(r.oid, true) as condef\n"
							  "FROM pg_catalog.pg_constraint r\n"
					"WHERE r.conrelid = '%s' AND r.contype = 'f' ORDER BY 1",
							  oid);
			result = PSQLexec(buf.data, false);
			if (!result)
				goto error_return;
			else
				tuples = PQntuples(result);

			if (tuples > 0)
			{
				printTableAddFooter(&cont, _("Foreign-key constraints:"));
				for (i = 0; i < tuples; i++)
				{
					/* untranslated constraint name and def */
					printfPQExpBuffer(&buf, "    \"%s\" %s",
									  PQgetvalue(result, i, 0),
									  PQgetvalue(result, i, 1));

					printTableAddFooter(&cont, buf.data);
				}
			}
			PQclear(result);
		}

		/* print incoming foreign-key references (none if no triggers) */
		if (tableinfo.hastriggers)
		{
			printfPQExpBuffer(&buf,
						   "SELECT conname, conrelid::pg_catalog.regclass,\n"
				 "  pg_catalog.pg_get_constraintdef(c.oid, true) as condef\n"
							  "FROM pg_catalog.pg_constraint c\n"
				   "WHERE c.confrelid = '%s' AND c.contype = 'f' ORDER BY 1",
							  oid);
			result = PSQLexec(buf.data, false);
			if (!result)
				goto error_return;
			else
				tuples = PQntuples(result);

			if (tuples > 0)
			{
				printTableAddFooter(&cont, _("Referenced by:"));
				for (i = 0; i < tuples; i++)
				{
					printfPQExpBuffer(&buf, "    TABLE \"%s\" CONSTRAINT \"%s\" %s",
									  PQgetvalue(result, i, 1),
									  PQgetvalue(result, i, 0),
									  PQgetvalue(result, i, 2));

					printTableAddFooter(&cont, buf.data);
				}
			}
			PQclear(result);
		}



		/* print rules */
		if (tableinfo.hasrules)
		{
			if (pset.sversion >= 80300)
			{
				printfPQExpBuffer(&buf,
								  "SELECT r.rulename, trim(trailing ';' from pg_catalog.pg_get_ruledef(r.oid, true)), "
								  "ev_enabled\n"
								  "FROM pg_catalog.pg_rewrite r\n"
								  "WHERE r.ev_class = '%s' ORDER BY 1",
								  oid);
			}
			else
			{
				printfPQExpBuffer(&buf,
								  "SELECT r.rulename, trim(trailing ';' from pg_catalog.pg_get_ruledef(r.oid, true)), "
								  "'O'::char AS ev_enabled\n"
								  "FROM pg_catalog.pg_rewrite r\n"
								  "WHERE r.ev_class = '%s' ORDER BY 1",
								  oid);
			}
			result = PSQLexec(buf.data, false);
			if (!result)
				goto error_return;
			else
				tuples = PQntuples(result);

			if (tuples > 0)
			{
				bool		have_heading;
				int			category;

				for (category = 0; category < 4; category++)
				{
					have_heading = false;

					for (i = 0; i < tuples; i++)
					{
						const char *ruledef;
						bool		list_rule = false;

						switch (category)
						{
							case 0:
								if (*PQgetvalue(result, i, 2) == 'O')
									list_rule = true;
								break;
							case 1:
								if (*PQgetvalue(result, i, 2) == 'D')
									list_rule = true;
								break;
							case 2:
								if (*PQgetvalue(result, i, 2) == 'A')
									list_rule = true;
								break;
							case 3:
								if (*PQgetvalue(result, i, 2) == 'R')
									list_rule = true;
								break;
						}
						if (!list_rule)
							continue;

						if (!have_heading)
						{
							switch (category)
							{
								case 0:
									printfPQExpBuffer(&buf, _("Rules:"));
									break;
								case 1:
									printfPQExpBuffer(&buf, _("Disabled rules:"));
									break;
								case 2:
									printfPQExpBuffer(&buf, _("Rules firing always:"));
									break;
								case 3:
									printfPQExpBuffer(&buf, _("Rules firing on replica only:"));
									break;
							}
							printTableAddFooter(&cont, buf.data);
							have_heading = true;
						}

						/* Everything after "CREATE RULE" is echoed verbatim */
						ruledef = PQgetvalue(result, i, 1);
						ruledef += 12;
						printfPQExpBuffer(&buf, "    %s", ruledef);
						printTableAddFooter(&cont, buf.data);
					}
				}
			}
			PQclear(result);
		}

		/* print triggers (but only user-defined triggers) */
		if (tableinfo.hastriggers)
		{
			printfPQExpBuffer(&buf,
							  "SELECT t.tgname, "
							  "pg_catalog.pg_get_triggerdef(t.oid%s), "
							  "t.tgenabled\n"
							  "FROM pg_catalog.pg_trigger t\n"
							  "WHERE t.tgrelid = '%s' AND ",
							  (pset.sversion >= 90000 ? ", true" : ""),
							  oid);
			if (pset.sversion >= 90000)
				appendPQExpBuffer(&buf, "NOT t.tgisinternal");
			else if (pset.sversion >= 80300)
				appendPQExpBuffer(&buf, "t.tgconstraint = 0");
			else
				appendPQExpBuffer(&buf,
								  "(NOT tgisconstraint "
								  " OR NOT EXISTS"
								  "  (SELECT 1 FROM pg_catalog.pg_depend d "
								  "   JOIN pg_catalog.pg_constraint c ON (d.refclassid = c.tableoid AND d.refobjid = c.oid) "
								  "   WHERE d.classid = t.tableoid AND d.objid = t.oid AND d.deptype = 'i' AND c.contype = 'f'))");
			appendPQExpBuffer(&buf, "\nORDER BY 1");

			result = PSQLexec(buf.data, false);
			if (!result)
				goto error_return;
			else
				tuples = PQntuples(result);

			if (tuples > 0)
			{
				bool		have_heading;
				int			category;

				/*
				 * split the output into 4 different categories. Enabled
				 * triggers, disabled triggers and the two special ALWAYS and
				 * REPLICA configurations.
				 */
				for (category = 0; category < 4; category++)
				{
					have_heading = false;
					for (i = 0; i < tuples; i++)
					{
						bool		list_trigger;
						const char *tgdef;
						const char *usingpos;
						const char *tgenabled;

						/*
						 * Check if this trigger falls into the current
						 * category
						 */
						tgenabled = PQgetvalue(result, i, 2);
						list_trigger = false;
						switch (category)
						{
							case 0:
								if (*tgenabled == 'O' || *tgenabled == 't')
									list_trigger = true;
								break;
							case 1:
								if (*tgenabled == 'D' || *tgenabled == 'f')
									list_trigger = true;
								break;
							case 2:
								if (*tgenabled == 'A')
									list_trigger = true;
								break;
							case 3:
								if (*tgenabled == 'R')
									list_trigger = true;
								break;
						}
						if (list_trigger == false)
							continue;

						/* Print the category heading once */
						if (have_heading == false)
						{
							switch (category)
							{
								case 0:
									printfPQExpBuffer(&buf, _("Triggers:"));
									break;
								case 1:
									printfPQExpBuffer(&buf, _("Disabled triggers:"));
									break;
								case 2:
									printfPQExpBuffer(&buf, _("Triggers firing always:"));
									break;
								case 3:
									printfPQExpBuffer(&buf, _("Triggers firing on replica only:"));
									break;

							}
							printTableAddFooter(&cont, buf.data);
							have_heading = true;
						}

						/* Everything after "TRIGGER" is echoed verbatim */
						tgdef = PQgetvalue(result, i, 1);
						usingpos = strstr(tgdef, " TRIGGER ");
						if (usingpos)
							tgdef = usingpos + 9;

						printfPQExpBuffer(&buf, "    %s", tgdef);
						printTableAddFooter(&cont, buf.data);
					}
				}
			}
			PQclear(result);
		}

		/* print inherited tables */
		printfPQExpBuffer(&buf, "SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhparent AND i.inhrelid = '%s' ORDER BY inhseqno", oid);

		result = PSQLexec(buf.data, false);
		if (!result)
			goto error_return;
		else
			tuples = PQntuples(result);

		for (i = 0; i < tuples; i++)
		{
			const char *s = _("Inherits");

			if (i == 0)
				printfPQExpBuffer(&buf, "%s: %s", s, PQgetvalue(result, i, 0));
			else
				printfPQExpBuffer(&buf, "%*s  %s", (int) strlen(s), "", PQgetvalue(result, i, 0));
			if (i < tuples - 1)
				appendPQExpBuffer(&buf, ",");

			printTableAddFooter(&cont, buf.data);
		}
		PQclear(result);

		/* print child tables */
		if (pset.sversion >= 80300)
			printfPQExpBuffer(&buf, "SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhrelid AND i.inhparent = '%s' ORDER BY c.oid::pg_catalog.regclass::pg_catalog.text;", oid);
		else
			printfPQExpBuffer(&buf, "SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhrelid AND i.inhparent = '%s' ORDER BY c.relname;", oid);

		result = PSQLexec(buf.data, false);
		if (!result)
			goto error_return;
		else
			tuples = PQntuples(result);

		if (!verbose)
		{
			/* print the number of child tables, if any */
			if (tuples > 0)
			{
				printfPQExpBuffer(&buf, _("Number of child tables: %d (Use \\d+ to list them.)"), tuples);
				printTableAddFooter(&cont, buf.data);
			}
		}
		else
		{
			/* display the list of child tables */
			const char *ct = _("Child tables");

			for (i = 0; i < tuples; i++)
			{
				if (i == 0)
					printfPQExpBuffer(&buf, "%s: %s",
									  ct, PQgetvalue(result, i, 0));
				else
					printfPQExpBuffer(&buf, "%*s  %s",
									  (int) strlen(ct), "",
									  PQgetvalue(result, i, 0));
				if (i < tuples - 1)
					appendPQExpBuffer(&buf, ",");

				printTableAddFooter(&cont, buf.data);
			}
		}
		PQclear(result);

		/* Table type */
		if (tableinfo.reloftype)
		{
			printfPQExpBuffer(&buf, _("Typed table of type: %s"), tableinfo.reloftype);
			printTableAddFooter(&cont, buf.data);
		}

		/* OIDs and options */
		if (verbose)
		{
			const char *s = _("Has OIDs");

			printfPQExpBuffer(&buf, "%s: %s", s,
							  (tableinfo.hasoids ? _("yes") : _("no")));
			printTableAddFooter(&cont, buf.data);

			/* print reloptions */
			if (pset.sversion >= 80200)
			{
				if (tableinfo.reloptions && tableinfo.reloptions[0] != '\0')
				{
					const char *t = _("Options");

					printfPQExpBuffer(&buf, "%s: %s", t,
									  tableinfo.reloptions);
					printTableAddFooter(&cont, buf.data);
				}
			}
		}

		/* mpp addition start: dump distributed by clause */
		resetPQExpBuffer(&tmpbuf);
		add_distributed_by_footer(oid, &tmpbuf, buf);
		printTableAddFooter(&cont, tmpbuf.data);

		/* print 'partition by' clause */
		if (tuples > 0)
		{
			resetPQExpBuffer(&tmpbuf);
			add_partition_by_footer(oid, &tmpbuf, &buf);
			printTableAddFooter(&cont, tmpbuf.data);
		}

		add_tablespace_footer(&cont, tableinfo.relkind, tableinfo.tablespace,
							  true);
	}

	printTable(&cont, pset.queryFout, pset.logfile);
	printTableCleanup(&cont);

	retval = true;

error_return:

	/* clean up */
	if (printTableInitialized)
		printTableCleanup(&cont);
	termPQExpBuffer(&buf);
	termPQExpBuffer(&title);
	termPQExpBuffer(&tmpbuf);

	if (seq_values)
	{
		for (ptr = seq_values; *ptr; ptr++)
			free(*ptr);
		free(seq_values);
	}

	if (modifiers)
	{
		for (ptr = modifiers; *ptr; ptr++)
			free(*ptr);
		free(modifiers);
	}

	if (view_def)
		free(view_def);

	if (tableinfo.compressionType)
		free(tableinfo.compressionType);
	if (tableinfo.compressionLevel)
		free(tableinfo.compressionLevel);
	if (tableinfo.blockSize)
		free(tableinfo.blockSize);
	if (tableinfo.pageSize)
		free(tableinfo.pageSize);
	if (tableinfo.rowgroupSize)
		free(tableinfo.rowgroupSize);
	if (tableinfo.checksum)
		free(tableinfo.checksum);

	if (res)
		PQclear(res);

	return retval;
}

static int
add_distributed_by_footer(const char* oid, PQExpBufferData *inoutbuf, PQExpBufferData buf)
{
	PGresult   *result1 = NULL,
			   *result2 = NULL;

	printfPQExpBuffer(&buf,
			 "SELECT attrnums\n"
					  "FROM pg_catalog.gp_distribution_policy t\n"
					  "WHERE localoid = '%s' ",
					  oid);

	result1 = PSQLexec(buf.data, false);
	if (!result1)
	{
		/* Error:  Well, so what?  Best to continue */
	}
	else
	{
		int is_distributed = PQntuples(result1);
		if (is_distributed)
		{
			char *col;
			char *dist_columns = PQgetvalue(result1, 0, 0);
			char *dist_colname;
			if(dist_columns && strlen(dist_columns) > 0)
			{
				PQExpBufferData tempbuf;

				initPQExpBuffer(&tempbuf);
				dist_columns[strlen(dist_columns)-1] = '\0'; /* remove '}' */
				dist_columns++;  /* skip '{' */

				/* Get the attname for the first distribution column.*/
				printfPQExpBuffer(&tempbuf,
					"SELECT attname FROM pg_attribute \n"
					"WHERE attrelid = '%s' \n"
					"AND attnum = '%d' ",
					oid,
					atoi(dist_columns));
				result2 = PSQLexec(tempbuf.data, false);
				if (!result2)
					return 1;
				dist_colname = PQgetvalue(result2, 0, 0);
				if (!dist_colname)
					return 1;
				printfPQExpBuffer(&buf, "Distributed by: (%s",
								  dist_colname);
				PQclear(result2);
				dist_colname = NULL;
				col = strchr(dist_columns,',');

				while(col!=NULL)
				{
					col++;
					/* Get the attname for next distribution columns.*/
					printfPQExpBuffer(&tempbuf,
						"SELECT attname FROM pg_attribute \n"
						"WHERE attrelid = '%s' \n"
						"AND attnum = '%d' ",
						oid,
						atoi(col));
					result2 = PSQLexec(tempbuf.data, false);
					if (!result2)
						return 1;
					dist_colname = PQgetvalue(result2, 0, 0);
					if (!dist_colname)
						return 1;
					appendPQExpBuffer(&buf, ", %s", dist_colname);
					PQclear(result2);
					col = strchr(col,',');
				}
				appendPQExpBuffer(&buf, ")");
				termPQExpBuffer(&tempbuf);
			}
			else
			{
				printfPQExpBuffer(&buf, "Distributed randomly");
			}

			appendPQExpBuffer(inoutbuf, "%s", pg_strdup(buf.data));
		}

		PQclear(result1);
	}

	return 0; /* success */
}

/*
 * Add a 'partition by' description to the footer.
 */
static int
add_partition_by_footer(const char* oid, PQExpBufferData *inoutbuf, PQExpBufferData *buf)
{
	PGresult	*result = NULL;

	/* check if current relation is root partition, if it is root partition, at least 1 row returns */
	printfPQExpBuffer(buf, "SELECT parrelid FROM pg_catalog.pg_partition WHERE parrelid = '%s'", oid);
	result = PSQLexec(buf->data, false);

	if (!result)
		return 1;
	int nRows = PQntuples(result);
	int nPartKey = 0;

	PQclear(result);

	if (nRows)
	{
		/* query partition key on the root partition */
		printfPQExpBuffer(buf,
			"WITH att_arr AS (SELECT unnest(paratts) \n"
			"	FROM pg_catalog.pg_partition p \n"
			"	WHERE p.parrelid = '%s' AND p.parlevel = 0 AND p.paristemplate = false), \n"
			"idx_att AS (SELECT row_number() OVER() AS idx, unnest AS att_num FROM att_arr) \n"
			"SELECT attname FROM pg_catalog.pg_attribute, idx_att \n"
			"	WHERE attrelid='%s' AND attnum = att_num ORDER BY idx ",
			oid, oid);
	}
	else
	{
		/* query partition key on the intermediate partition */
		printfPQExpBuffer(buf,
			"WITH att_arr AS (SELECT unnest(paratts) FROM pg_catalog.pg_partition p, \n"
			"	(SELECT parrelid, parlevel \n"
			"		FROM pg_catalog.pg_partition p, pg_catalog.pg_partition_rule pr \n"
			"		WHERE pr.parchildrelid='%s' AND p.oid = pr.paroid) AS v \n"
			"	WHERE p.parrelid = v.parrelid AND p.parlevel = v.parlevel+1 AND p.paristemplate = false), \n"
			"idx_att AS (SELECT row_number() OVER() AS idx, unnest AS att_num FROM att_arr) \n"
			"SELECT attname FROM pg_catalog.pg_attribute, idx_att \n"
			"	WHERE attrelid='%s' AND attnum = att_num ORDER BY idx ",
			oid, oid);
	}

	result = PSQLexec(buf->data, false);
	if (!result)
		return 1;
	nPartKey = PQntuples(result);

	if (nPartKey)
	{
		char *partColName;
		int i = 0;
		appendPQExpBuffer(inoutbuf, "Partition by: (");
		for (i = 0; i < nPartKey; i++)
		{
			if (i > 0)
				appendPQExpBuffer(inoutbuf, ", ");
			partColName = PQgetvalue(result, i, 0);

			if (!partColName)
			{
				resetPQExpBuffer(inoutbuf);
				return 1;
			}
			appendPQExpBuffer(inoutbuf, "%s", partColName);
		}
		appendPQExpBuffer(inoutbuf, ")");
	}

	PQclear(result);

	return 0; /* success */
}

/*
 * Add a tablespace description to a footer.  If 'newline' is true, it is added
 * in a new line; otherwise it's appended to the current value of the last
 * footer.
 */
static void
add_tablespace_footer(printTableContent *const cont, char relkind,
					  Oid tablespace, const bool newline)
{
	/* relkinds for which we support tablespaces */
	if (relkind == 'r' || relkind == 'i')
	{
		/*
		 * We ignore the database default tablespace so that users not using
		 * tablespaces don't need to know about them.  This case also covers
		 * pre-8.0 servers, for which tablespace will always be 0.
		 */
		if (tablespace != 0)
		{
			PGresult   *result = NULL;
			PQExpBufferData buf;

			initPQExpBuffer(&buf);
			printfPQExpBuffer(&buf,
							  "SELECT spcname FROM pg_catalog.pg_tablespace\n"
							  "WHERE oid = '%u'", tablespace);
			result = PSQLexec(buf.data, false);
			if (!result)
				return;
			/* Should always be the case, but.... */
			if (PQntuples(result) > 0)
			{
				if (newline)
				{
					/* Add the tablespace as a new footer */
					printfPQExpBuffer(&buf, _("Tablespace: \"%s\""),
									  PQgetvalue(result, 0, 0));
					printTableAddFooter(cont, buf.data);
				}
				else
				{
					/* Append the tablespace to the latest footer */
					printfPQExpBuffer(&buf, "%s", cont->footer->data);

					/*
					 * translator: before this string there's an index
					 * description like '"foo_pkey" PRIMARY KEY, btree (a)'
					 */
					appendPQExpBuffer(&buf, _(", tablespace \"%s\""),
									  PQgetvalue(result, 0, 0));
					printTableSetFooter(cont, buf.data);
				}
			}
			PQclear(result);
			termPQExpBuffer(&buf);
		}
	}
}

/*
 * \du or \dg
 *
 * Describes roles.  Any schema portion of the pattern is ignored.
 */
bool
describeRoles(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printTableContent cont;
	printTableOpt myopt = pset.popt.topt;
	int			ncols = 3;
	int			nrows = 0;
	int			i;
	int			conns;
	const char	align = 'l';
	char	  **attr;

	initPQExpBuffer(&buf);

	if (pset.sversion >= 80100)
	{
		printfPQExpBuffer(&buf,
						  "SELECT r.rolname, r.rolsuper, r.rolinherit,\n"
						  "  r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,\n"
						  "  r.rolconnlimit,\n"
						  "  ARRAY(SELECT b.rolname\n"
						  "        FROM pg_catalog.pg_auth_members m\n"
				 "        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)\n"
						  "        WHERE m.member = r.oid) as memberof");

		if (verbose && pset.sversion >= 80200)
		{
			appendPQExpBufferStr(&buf, "\n, pg_catalog.shobj_description(r.oid, 'pg_authid') AS description");
			ncols++;
		}

		appendPQExpBufferStr(&buf, "\nFROM pg_catalog.pg_roles r\n");

		processSQLNamePattern(pset.db, &buf, pattern, false, false,
							  NULL, "r.rolname", NULL, NULL);
	}
	else
	{
		printfPQExpBuffer(&buf,
						  "SELECT u.usename AS rolname,\n"
						  "  u.usesuper AS rolsuper,\n"
						  "  true AS rolinherit, false AS rolcreaterole,\n"
					 "  u.usecreatedb AS rolcreatedb, true AS rolcanlogin,\n"
						  "  -1 AS rolconnlimit,\n"
						  "  ARRAY(SELECT g.groname FROM pg_catalog.pg_group g WHERE u.usesysid = ANY(g.grolist)) as memberof"
						  "\nFROM pg_catalog.pg_user u\n");

		processSQLNamePattern(pset.db, &buf, pattern, false, false,
							  NULL, "u.usename", NULL, NULL);
	}

	appendPQExpBuffer(&buf, "ORDER BY 1;");

	res = PSQLexec(buf.data, false);
	if (!res)
		return false;

	nrows = PQntuples(res);
	attr = pg_malloc_zero((nrows + 1) * sizeof(*attr));

	printTableInit(&cont, &myopt, _("List of roles"), ncols, nrows);

	printTableAddHeader(&cont, gettext_noop("Role name"), true, align);
	printTableAddHeader(&cont, gettext_noop("Attributes"), true, align);
	printTableAddHeader(&cont, gettext_noop("Member of"), true, align);

	if (verbose && pset.sversion >= 80200)
		printTableAddHeader(&cont, gettext_noop("Description"), true, align);

	for (i = 0; i < nrows; i++)
	{
		printTableAddCell(&cont, PQgetvalue(res, i, 0), false, false);

		resetPQExpBuffer(&buf);
		if (strcmp(PQgetvalue(res, i, 1), "t") == 0)
			add_role_attribute(&buf, _("Superuser"));

		if (strcmp(PQgetvalue(res, i, 2), "t") != 0)
			add_role_attribute(&buf, _("No inheritance"));

		if (strcmp(PQgetvalue(res, i, 3), "t") == 0)
			add_role_attribute(&buf, _("Create role"));

		if (strcmp(PQgetvalue(res, i, 4), "t") == 0)
			add_role_attribute(&buf, _("Create DB"));

		if (strcmp(PQgetvalue(res, i, 5), "t") != 0)
			add_role_attribute(&buf, _("Cannot login"));

		conns = atoi(PQgetvalue(res, i, 6));
		if (conns >= 0)
		{
			if (buf.len > 0)
				appendPQExpBufferStr(&buf, "\n");

			if (conns == 0)
				appendPQExpBuffer(&buf, _("No connections"));
			else
				appendPQExpBuffer(&buf, ngettext("%d connection",
												 "%d connections",
												 conns),
								  conns);
		}

		attr[i] = pg_strdup(buf.data);

		printTableAddCell(&cont, attr[i], false, false);

		printTableAddCell(&cont, PQgetvalue(res, i, 7), false, false);

		if (verbose && pset.sversion >= 80200)
			printTableAddCell(&cont, PQgetvalue(res, i, 8), false, false);
	}
	termPQExpBuffer(&buf);

	printTable(&cont, pset.queryFout, pset.logfile);
	printTableCleanup(&cont);

	for (i = 0; i < nrows; i++)
		free(attr[i]);
	free(attr);

	PQclear(res);
	return true;
}

static void
add_role_attribute(PQExpBuffer buf, const char *const str)
{
	if (buf->len > 0)
		appendPQExpBufferStr(buf, ", ");

	appendPQExpBufferStr(buf, str);
}

/*
 * \drds
 */
bool
listDbRoleSettings(const char *pattern, const char *pattern2)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);

	if (pset.sversion >= 90000)
	{
		/* ACHOI: havewhere is false */
		bool		havewhere = false;

		printfPQExpBuffer(&buf, "SELECT rolname AS role, datname AS database,\n"
				"pg_catalog.array_to_string(setconfig, E'\\n') AS settings\n"
						  "FROM pg_db_role_setting AS s\n"
				   "LEFT JOIN pg_database ON pg_database.oid = setdatabase\n"
						  "LEFT JOIN pg_roles ON pg_roles.oid = setrole\n");

		/* ACHOI: psql 9.0 assing the havewhere here */
		processSQLNamePattern(pset.db, &buf, pattern, false, false,
									   NULL, "pg_roles.rolname", NULL, NULL);
		processSQLNamePattern(pset.db, &buf, pattern2, havewhere, false,
							  NULL, "pg_database.datname", NULL, NULL);
		appendPQExpBufferStr(&buf, "ORDER BY role, database");
	}
	else
	{
		fprintf(pset.queryFout,
		_("No per-database role settings support in this server version.\n"));
		return false;
	}

	res = PSQLexec(buf.data, false);
	if (!res)
		return false;

	if (PQntuples(res) == 0 && !pset.quiet)
	{
		if (pattern)
			fprintf(pset.queryFout, _("No matching settings found.\n"));
		else
			fprintf(pset.queryFout, _("No settings found.\n"));
	}
	else
	{
		myopt.nullPrint = NULL;
		myopt.title = _("List of settings");
		myopt.translate_header = true;

		printQuery(res, &myopt, pset.queryFout, pset.logfile);
	}

	PQclear(res);
	resetPQExpBuffer(&buf);
	return true;
}


/*
 * listTables()
 *
 * handler for \dt, \di, etc.
 *
 * tabtypes is an array of characters, specifying what info is desired:
 * t - tables
 * i - indexes
 * v - views
 * s - sequences
 * r - foreign tables   *GPDB only*
 * (any order of the above is fine)
 * If tabtypes is empty, we default to \dtvsr.
 */
bool
listTables(const char *tabtypes, const char *pattern, bool verbose, bool showSystem)
{
	bool		showChildren = true;
	bool		showTables = strchr(tabtypes, 't') != NULL;
	bool		showIndexes = strchr(tabtypes, 'i') != NULL;
	bool		showViews = strchr(tabtypes, 'v') != NULL;
	bool		showSeq = strchr(tabtypes, 's') != NULL;
	bool		showExternal = strchr(tabtypes, 'x') != NULL;
	bool		showForeign = strchr(tabtypes, 'r') != NULL;

	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;
	static const bool translate_columns[] = {false, false, true, false, false, false, false};

	if (!(showTables || showIndexes || showViews || showSeq || showExternal || showForeign))
		showTables = showViews = showSeq = showExternal = showForeign = true;

	if (strchr(tabtypes, 'P') != NULL)
	{
		showTables = true;
		showChildren = false;
	}

	initPQExpBuffer(&buf);

	/*
	 * Note: as of Pg 8.2, we no longer use relkind 's', but we keep it here
	 * for backwards compatibility.
	 */
	printfPQExpBuffer(&buf,
					  "SELECT n.nspname as \"%s\",\n"
					  "  c.relname as \"%s\",\n"
					  "  CASE c.relkind WHEN 'r' THEN '%s' WHEN 'v' THEN '%s' WHEN 'i' THEN '%s' WHEN 'S' THEN '%s' WHEN 's' THEN '%s' END as \"%s\",\n"
					  "  pg_catalog.pg_get_userbyid(c.relowner) as \"%s\"",
					  gettext_noop("Schema"),
					  gettext_noop("Name"),
					  gettext_noop("table"),
					  gettext_noop("view"),
					  gettext_noop("index"),
					  gettext_noop("sequence"),
					  gettext_noop("special"),
					  gettext_noop("Type"),
					  gettext_noop("Owner"));

	if (isGPDB())   /* GPDB? */
		appendPQExpBuffer(&buf,
				  ", CASE c.relstorage WHEN 'h' THEN '%s' WHEN 'x' THEN '%s' WHEN 'a' "
				  "THEN '%s' WHEN 'v' THEN '%s' WHEN 'c' THEN '%s' WHEN 'p' THEN '%s' WHEN 'f' THEN '%s' END as \"%s\"\n",
				  gettext_noop("heap"), gettext_noop("external"), gettext_noop("append only"), gettext_noop("none"), gettext_noop("append only columnar"), gettext_noop("parquet"), gettext_noop("foreign"), gettext_noop("Storage"));

	if (showIndexes)
		appendPQExpBuffer(&buf,
						  ",\n c2.relname as \"%s\"",
						  gettext_noop("Table"));

	/*
	 * GPDB:  Stupid check to see if we are using old-ascii, and therefore are possibly running regression tests, which haven't
	 * been updated to expect the Size column in the result
	 */
	if (verbose && pset.sversion >= 80100 && strcmp(get_line_style(&pset.popt.topt)->name,"old-ascii") != 0)
		appendPQExpBuffer(&buf,
						  ",\n  pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(c.oid)) as \"%s\"",
						  gettext_noop("Size"));
	if (verbose)
		appendPQExpBuffer(&buf,
			  ",\n  pg_catalog.obj_description(c.oid, 'pg_class') as \"%s\"",
						  gettext_noop("Description"));

	appendPQExpBuffer(&buf,
					  "\nFROM pg_catalog.pg_class c"
	 "\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace");
	if (showIndexes)
		appendPQExpBuffer(&buf,
			 "\n     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid"
		   "\n     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid");

	appendPQExpBuffer(&buf, "\nWHERE c.relkind IN (");
	if (showTables || showExternal || showForeign)
		appendPQExpBuffer(&buf, "'r',");
	if (showViews)
		appendPQExpBuffer(&buf, "'v',");
	if (showIndexes)
		appendPQExpBuffer(&buf, "'i',");
	if (showSeq)
		appendPQExpBuffer(&buf, "'S',");
	if (showSystem || pattern)
		appendPQExpBuffer(&buf, "'s',");		/* was RELKIND_SPECIAL in <=
												 * 8.1 */
	appendPQExpBuffer(&buf, "''");		/* dummy */
	appendPQExpBuffer(&buf, ")\n");

    if (isGPDB())   /* GPDB? */
    {
	appendPQExpBuffer(&buf, "AND c.relstorage IN (");
	if (showTables || showIndexes || showSeq || (showSystem && showTables))
		appendPQExpBuffer(&buf, "'h', 'a', 'c', 'p',");
	if (showExternal)
		appendPQExpBuffer(&buf, "'x',");
	if (showForeign)
		appendPQExpBuffer(&buf, "'f',");
	if (showViews)
		appendPQExpBuffer(&buf, "'v',");
	appendPQExpBuffer(&buf, "''");		/* dummy */
	appendPQExpBuffer(&buf, ")\n");
    }

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	/*
	 * TOAST objects are suppressed unconditionally.  Since we don't provide
	 * any way to select relkind 't' above, we would never show toast tables
	 * in any case; it seems a bit confusing to allow their indexes to be
	 * shown. Use plain \d if you really need to look at a TOAST table/index.
	 */
	appendPQExpBuffer(&buf, "      AND n.nspname !~ '^pg_toast'\n");

	if (!showChildren)
		appendPQExpBuffer(&buf, "      AND c.oid NOT IN (select inhrelid from pg_catalog.pg_inherits)\n");

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "c.relname", NULL,
						  "pg_catalog.pg_table_is_visible(c.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1,2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	if (PQntuples(res) == 0 && !pset.quiet)
	{
		if (pattern)
			fprintf(pset.queryFout, _("No matching relations found.\n"));
		else
			fprintf(pset.queryFout, _("No relations found.\n"));
	}
	else
	{
		myopt.nullPrint = NULL;
		myopt.title = _("List of relations");
		myopt.translate_header = true;
		myopt.translate_columns = translate_columns;

		printQuery(res, &myopt, pset.queryFout, pset.logfile);
	}

	PQclear(res);
	return true;
}


/*
 * \dD
 *
 * Describes domains.
 */
bool
listDomains(const char *pattern, bool showSystem)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT n.nspname as \"%s\",\n"
					  "       t.typname as \"%s\",\n"
	 "       pg_catalog.format_type(t.typbasetype, t.typtypmod) as \"%s\",\n"
					  "       CASE WHEN t.typnotnull AND t.typdefault IS NOT NULL THEN 'not null default '||t.typdefault\n"
	"            WHEN t.typnotnull AND t.typdefault IS NULL THEN 'not null'\n"
					  "            WHEN NOT t.typnotnull AND t.typdefault IS NOT NULL THEN 'default '||t.typdefault\n"
					  "            ELSE ''\n"
					  "       END as \"%s\",\n"
					  "       pg_catalog.array_to_string(ARRAY(\n"
					  "         SELECT pg_catalog.pg_get_constraintdef(r.oid, true) FROM pg_catalog.pg_constraint r WHERE t.oid = r.contypid\n"
					  "       ), ' ') as \"%s\"\n"
					  "FROM pg_catalog.pg_type t\n"
	   "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n"
					  "WHERE t.typtype = 'd'\n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"),
					  gettext_noop("Type"),
					  gettext_noop("Modifier"),
					  gettext_noop("Check"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "t.typname", NULL,
						  "pg_catalog.pg_type_is_visible(t.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of domains");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}

/*
 * \dc
 *
 * Describes conversions.
 */
bool
listConversions(const char *pattern, bool showSystem)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;
	static const bool translate_columns[] = {false, false, false, false, true};

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT n.nspname AS \"%s\",\n"
					  "       c.conname AS \"%s\",\n"
	   "       pg_catalog.pg_encoding_to_char(c.conforencoding) AS \"%s\",\n"
		"       pg_catalog.pg_encoding_to_char(c.contoencoding) AS \"%s\",\n"
					  "       CASE WHEN c.condefault THEN '%s'\n"
					  "       ELSE '%s' END AS \"%s\"\n"
			   "FROM pg_catalog.pg_conversion c, pg_catalog.pg_namespace n\n"
					  "WHERE n.oid = c.connamespace\n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"),
					  gettext_noop("Source"),
					  gettext_noop("Destination"),
					  gettext_noop("yes"), gettext_noop("no"),
					  gettext_noop("Default?"));

	if (!showSystem && !pattern)
		appendPQExpBuffer(&buf, "      AND n.nspname <> 'pg_catalog'\n"
						  "      AND n.nspname <> 'information_schema'\n");

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "c.conname", NULL,
						  "pg_catalog.pg_conversion_is_visible(c.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of conversions");
	myopt.translate_header = true;
	myopt.translate_columns = translate_columns;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}

/*
 * \dC
 *
 * Describes casts.
 */
bool
listCasts(const char *pattern)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;
	static const bool translate_columns[] = {false, false, false, true};

	initPQExpBuffer(&buf);

	/*
	 * We need a left join to pg_proc for binary casts; the others are just
	 * paranoia.  Also note that we don't attempt to localize '(binary
	 * coercible)', because there's too much risk of gettext translating a
	 * function name that happens to match some string in the PO database.
	 */
	printfPQExpBuffer(&buf,
			   "SELECT pg_catalog.format_type(castsource, NULL) AS \"%s\",\n"
			   "       pg_catalog.format_type(casttarget, NULL) AS \"%s\",\n"
				  "       CASE WHEN castfunc = 0 THEN '(binary coercible)'\n"
					  "            ELSE p.proname\n"
					  "       END as \"%s\",\n"
					  "       CASE WHEN c.castcontext = 'e' THEN '%s'\n"
					  "            WHEN c.castcontext = 'a' THEN '%s'\n"
					  "            ELSE '%s'\n"
					  "       END as \"%s\"\n"
				 "FROM pg_catalog.pg_cast c LEFT JOIN pg_catalog.pg_proc p\n"
					  "     ON c.castfunc = p.oid\n"
					  "     LEFT JOIN pg_catalog.pg_type ts\n"
					  "     ON c.castsource = ts.oid\n"
					  "     LEFT JOIN pg_catalog.pg_namespace ns\n"
					  "     ON ns.oid = ts.typnamespace\n"
					  "     LEFT JOIN pg_catalog.pg_type tt\n"
					  "     ON c.casttarget = tt.oid\n"
					  "     LEFT JOIN pg_catalog.pg_namespace nt\n"
					  "     ON nt.oid = tt.typnamespace\n"
					  "WHERE (true",
					  gettext_noop("Source type"),
					  gettext_noop("Target type"),
					  gettext_noop("Function"),
	  gettext_noop("no"), gettext_noop("in assignment"), gettext_noop("yes"),
					  gettext_noop("Implicit?"));

	/*
	 * Match name pattern against either internal or external name of either
	 * castsource or casttarget
	 */
	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "ns.nspname", "ts.typname",
						  "pg_catalog.format_type(ts.oid, NULL)",
						  "pg_catalog.pg_type_is_visible(ts.oid)");

	appendPQExpBuffer(&buf, ") OR (true");

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "nt.nspname", "tt.typname",
						  "pg_catalog.format_type(tt.oid, NULL)",
						  "pg_catalog.pg_type_is_visible(tt.oid)");

	appendPQExpBuffer(&buf, ")\nORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of casts");
	myopt.translate_header = true;
	myopt.translate_columns = translate_columns;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}

/*
 * \dn
 *
 * Describes schemas (namespaces)
 */
bool
listSchemas(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf,
					  "SELECT n.nspname AS \"%s\",\n"
					  "  pg_catalog.pg_get_userbyid(n.nspowner) AS \"%s\"",
					  gettext_noop("Name"),
					  gettext_noop("Owner"));

	if (verbose)
	{
		appendPQExpBuffer(&buf, ",\n  ");
		printACLColumn(&buf, "n.nspacl");
		appendPQExpBuffer(&buf,
		  ",\n  pg_catalog.obj_description(n.oid, 'pg_namespace') AS \"%s\"",
						  gettext_noop("Description"));
	}

	appendPQExpBuffer(&buf,
					  "\nFROM pg_catalog.pg_namespace n\n"
					  "WHERE	(n.nspname !~ '^pg_temp_' OR\n"
		   "		 n.nspname = (pg_catalog.current_schemas(true))[1])\n");		/* temp schema is first */

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  NULL, "n.nspname", NULL,
						  NULL);

	appendPQExpBuffer(&buf, "ORDER BY 1;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of schemas");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}


/*
 * \dFp
 * list text search parsers
 */
bool
listTSParsers(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	if (pset.sversion < 80300)
	{
		fprintf(stderr, _("The server (version %d.%d) does not support full text search.\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	if (verbose)
		return listTSParsersVerbose(pattern);

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT \n"
					  "  n.nspname as \"%s\",\n"
					  "  p.prsname as \"%s\",\n"
			"  pg_catalog.obj_description(p.oid, 'pg_ts_parser') as \"%s\"\n"
					  "FROM pg_catalog.pg_ts_parser p \n"
		   "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.prsnamespace\n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"),
					  gettext_noop("Description")
		);

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  "n.nspname", "p.prsname", NULL,
						  "pg_catalog.pg_ts_parser_is_visible(p.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of text search parsers");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}

/*
 * full description of parsers
 */
static bool
listTSParsersVerbose(const char *pattern)
{
	PQExpBufferData buf;
	PGresult   *res;
	int			i;

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT p.oid, \n"
					  "  n.nspname, \n"
					  "  p.prsname \n"
					  "FROM pg_catalog.pg_ts_parser p\n"
			"LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.prsnamespace\n"
		);

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  "n.nspname", "p.prsname", NULL,
						  "pg_catalog.pg_ts_parser_is_visible(p.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	if (PQntuples(res) == 0)
	{
		if (!pset.quiet)
			fprintf(stderr, _("Did not find any text search parser named \"%s\".\n"),
					pattern);
		PQclear(res);
		return false;
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		const char *oid;
		const char *nspname = NULL;
		const char *prsname;

		oid = PQgetvalue(res, i, 0);
		if (!PQgetisnull(res, i, 1))
			nspname = PQgetvalue(res, i, 1);
		prsname = PQgetvalue(res, i, 2);

		if (!describeOneTSParser(oid, nspname, prsname))
		{
			PQclear(res);
			return false;
		}

		if (cancel_pressed)
		{
			PQclear(res);
			return false;
		}
	}

	PQclear(res);
	return true;
}

static bool
describeOneTSParser(const char *oid, const char *nspname, const char *prsname)
{
	PQExpBufferData buf;
	PGresult   *res;
	char		title[1024];
	printQueryOpt myopt = pset.popt;
	static const bool translate_columns[] = {true, false, false};

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT '%s' AS \"%s\", \n"
					  "   p.prsstart::pg_catalog.regproc AS \"%s\", \n"
		  "   pg_catalog.obj_description(p.prsstart, 'pg_proc') as \"%s\" \n"
					  " FROM pg_catalog.pg_ts_parser p \n"
					  " WHERE p.oid = '%s' \n"
					  "UNION ALL \n"
					  "SELECT '%s', \n"
					  "   p.prstoken::pg_catalog.regproc, \n"
					"   pg_catalog.obj_description(p.prstoken, 'pg_proc') \n"
					  " FROM pg_catalog.pg_ts_parser p \n"
					  " WHERE p.oid = '%s' \n"
					  "UNION ALL \n"
					  "SELECT '%s', \n"
					  "   p.prsend::pg_catalog.regproc, \n"
					  "   pg_catalog.obj_description(p.prsend, 'pg_proc') \n"
					  " FROM pg_catalog.pg_ts_parser p \n"
					  " WHERE p.oid = '%s' \n"
					  "UNION ALL \n"
					  "SELECT '%s', \n"
					  "   p.prsheadline::pg_catalog.regproc, \n"
				 "   pg_catalog.obj_description(p.prsheadline, 'pg_proc') \n"
					  " FROM pg_catalog.pg_ts_parser p \n"
					  " WHERE p.oid = '%s' \n"
					  "UNION ALL \n"
					  "SELECT '%s', \n"
					  "   p.prslextype::pg_catalog.regproc, \n"
				  "   pg_catalog.obj_description(p.prslextype, 'pg_proc') \n"
					  " FROM pg_catalog.pg_ts_parser p \n"
					  " WHERE p.oid = '%s' \n",
					  gettext_noop("Start parse"),
					  gettext_noop("Method"),
					  gettext_noop("Function"),
					  gettext_noop("Description"),
					  oid,
					  gettext_noop("Get next token"),
					  oid,
					  gettext_noop("End parse"),
					  oid,
					  gettext_noop("Get headline"),
					  oid,
					  gettext_noop("Get token types"),
					  oid);

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	if (nspname)
		sprintf(title, _("Text search parser \"%s.%s\""), nspname, prsname);
	else
		sprintf(title, _("Text search parser \"%s\""), prsname);
	myopt.title = title;
	myopt.footers = NULL;
	myopt.default_footer = false;
	myopt.translate_header = true;
	myopt.translate_columns = translate_columns;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT t.alias as \"%s\", \n"
					  "  t.description as \"%s\" \n"
			  "FROM pg_catalog.ts_token_type( '%s'::pg_catalog.oid ) as t \n"
					  "ORDER BY 1;",
					  gettext_noop("Token name"),
					  gettext_noop("Description"),
					  oid);

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	if (nspname)
		sprintf(title, _("Token types for parser \"%s.%s\""), nspname, prsname);
	else
		sprintf(title, _("Token types for parser \"%s\""), prsname);
	myopt.title = title;
	myopt.footers = NULL;
	myopt.default_footer = true;
	myopt.translate_header = true;
	myopt.translate_columns = NULL;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}


/*
 * \dFd
 * list text search dictionaries
 */
bool
listTSDictionaries(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	if (pset.sversion < 80300)
	{
		fprintf(stderr, _("The server (version %d.%d) does not support full text search.\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT \n"
					  "  n.nspname as \"%s\",\n"
					  "  d.dictname as \"%s\",\n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"));

	if (verbose)
	{
		appendPQExpBuffer(&buf,
						  "  ( SELECT COALESCE(nt.nspname, '(null)')::pg_catalog.text || '.' || t.tmplname FROM \n"
						  "    pg_catalog.pg_ts_template t \n"
						  "			 LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid = t.tmplnamespace \n"
						  "			 WHERE d.dicttemplate = t.oid ) AS  \"%s\", \n"
						  "  d.dictinitoption as \"%s\", \n",
						  gettext_noop("Template"),
						  gettext_noop("Init options"));
	}

	appendPQExpBuffer(&buf,
			 "  pg_catalog.obj_description(d.oid, 'pg_ts_dict') as \"%s\"\n",
					  gettext_noop("Description"));

	appendPQExpBuffer(&buf, "FROM pg_catalog.pg_ts_dict d\n"
		 "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = d.dictnamespace\n");

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  "n.nspname", "d.dictname", NULL,
						  "pg_catalog.pg_ts_dict_is_visible(d.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of text search dictionaries");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}


/*
 * \dFt
 * list text search templates
 */
bool
listTSTemplates(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	if (pset.sversion < 80300)
	{
		fprintf(stderr, _("The server (version %d.%d) does not support full text search.\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	initPQExpBuffer(&buf);

	if (verbose)
		printfPQExpBuffer(&buf,
						  "SELECT \n"
						  "  n.nspname AS \"%s\",\n"
						  "  t.tmplname AS \"%s\",\n"
						  "  t.tmplinit::pg_catalog.regproc AS \"%s\",\n"
						  "  t.tmpllexize::pg_catalog.regproc AS \"%s\",\n"
		 "  pg_catalog.obj_description(t.oid, 'pg_ts_template') AS \"%s\"\n",
						  gettext_noop("Schema"),
						  gettext_noop("Name"),
						  gettext_noop("Init"),
						  gettext_noop("Lexize"),
						  gettext_noop("Description"));
	else
		printfPQExpBuffer(&buf,
						  "SELECT \n"
						  "  n.nspname AS \"%s\",\n"
						  "  t.tmplname AS \"%s\",\n"
		 "  pg_catalog.obj_description(t.oid, 'pg_ts_template') AS \"%s\"\n",
						  gettext_noop("Schema"),
						  gettext_noop("Name"),
						  gettext_noop("Description"));

	appendPQExpBuffer(&buf, "FROM pg_catalog.pg_ts_template t\n"
		 "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.tmplnamespace\n");

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  "n.nspname", "t.tmplname", NULL,
						  "pg_catalog.pg_ts_template_is_visible(t.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of text search templates");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}


/*
 * \dF
 * list text search configurations
 */
bool
listTSConfigs(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	if (pset.sversion < 80300)
	{
		fprintf(stderr, _("The server (version %d.%d) does not support full text search.\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	if (verbose)
		return listTSConfigsVerbose(pattern);

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT \n"
					  "   n.nspname as \"%s\",\n"
					  "   c.cfgname as \"%s\",\n"
		   "   pg_catalog.obj_description(c.oid, 'pg_ts_config') as \"%s\"\n"
					  "FROM pg_catalog.pg_ts_config c\n"
		  "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.cfgnamespace \n",
					  gettext_noop("Schema"),
					  gettext_noop("Name"),
					  gettext_noop("Description")
		);

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  "n.nspname", "c.cfgname", NULL,
						  "pg_catalog.pg_ts_config_is_visible(c.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of text search configurations");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}

static bool
listTSConfigsVerbose(const char *pattern)
{
	PQExpBufferData buf;
	PGresult   *res;
	int			i;

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT c.oid, c.cfgname,\n"
					  "   n.nspname, \n"
					  "   p.prsname, \n"
					  "   np.nspname as pnspname \n"
					  "FROM pg_catalog.pg_ts_config c \n"
	   "   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.cfgnamespace, \n"
					  " pg_catalog.pg_ts_parser p \n"
	  "   LEFT JOIN pg_catalog.pg_namespace np ON np.oid = p.prsnamespace \n"
					  "WHERE  p.oid = c.cfgparser\n"
		);

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "n.nspname", "c.cfgname", NULL,
						  "pg_catalog.pg_ts_config_is_visible(c.oid)");

	appendPQExpBuffer(&buf, "ORDER BY 3, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	if (PQntuples(res) == 0)
	{
		if (!pset.quiet)
			fprintf(stderr, _("Did not find any text search configuration named \"%s\".\n"),
					pattern);
		PQclear(res);
		return false;
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		const char *oid;
		const char *cfgname;
		const char *nspname = NULL;
		const char *prsname;
		const char *pnspname = NULL;

		oid = PQgetvalue(res, i, 0);
		cfgname = PQgetvalue(res, i, 1);
		if (!PQgetisnull(res, i, 2))
			nspname = PQgetvalue(res, i, 2);
		prsname = PQgetvalue(res, i, 3);
		if (!PQgetisnull(res, i, 4))
			pnspname = PQgetvalue(res, i, 4);

		if (!describeOneTSConfig(oid, nspname, cfgname, pnspname, prsname))
		{
			PQclear(res);
			return false;
		}

		if (cancel_pressed)
		{
			PQclear(res);
			return false;
		}
	}

	PQclear(res);
	return true;
}

static bool
describeOneTSConfig(const char *oid, const char *nspname, const char *cfgname,
					const char *pnspname, const char *prsname)
{
	PQExpBufferData buf,
				title;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);

	printfPQExpBuffer(&buf,
					  "SELECT \n"
					  "  ( SELECT t.alias FROM \n"
					  "    pg_catalog.ts_token_type(c.cfgparser) AS t \n"
					  "    WHERE t.tokid = m.maptokentype ) AS \"%s\", \n"
					  "  pg_catalog.btrim( \n"
				  "    ARRAY( SELECT mm.mapdict::pg_catalog.regdictionary \n"
					  "           FROM pg_catalog.pg_ts_config_map AS mm \n"
					  "           WHERE mm.mapcfg = m.mapcfg AND mm.maptokentype = m.maptokentype \n"
					  "           ORDER BY mapcfg, maptokentype, mapseqno \n"
					  "    ) :: pg_catalog.text , \n"
					  "  '{}') AS \"%s\" \n"
	 "FROM pg_catalog.pg_ts_config AS c, pg_catalog.pg_ts_config_map AS m \n"
					  "WHERE c.oid = '%s' AND m.mapcfg = c.oid \n"
					  "GROUP BY m.mapcfg, m.maptokentype, c.cfgparser \n"
					  "ORDER BY 1",
					  gettext_noop("Token"),
					  gettext_noop("Dictionaries"),
					  oid);

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	initPQExpBuffer(&title);

	if (nspname)
		appendPQExpBuffer(&title, _("Text search configuration \"%s.%s\""),
						  nspname, cfgname);
	else
		appendPQExpBuffer(&title, _("Text search configuration \"%s\""),
						  cfgname);

	if (pnspname)
		appendPQExpBuffer(&title, _("\nParser: \"%s.%s\""),
						  pnspname, prsname);
	else
		appendPQExpBuffer(&title, _("\nParser: \"%s\""),
						  prsname);

	myopt.nullPrint = NULL;
	myopt.title = title.data;
	myopt.footers = NULL;
	myopt.default_footer = false;
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	termPQExpBuffer(&title);

	PQclear(res);
	return true;
}


/*
 * \dew
 *
 * Describes foreign-data wrappers
 */
bool
listForeignDataWrappers(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	if (pset.sversion < 80400 && (pset.sversion < 80214 || !isGPDB()))  /* allow for Greenplum 8.2.x with FDWs */
	{
		fprintf(stderr, _("The server (version %d.%d) does not support foreign-data wrappers.\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf,
					  "SELECT fdwname AS \"%s\",\n"
					  "  pg_catalog.pg_get_userbyid(fdwowner) AS \"%s\",\n"
					  "  fdwvalidator::pg_catalog.regproc AS \"%s\"",
					  gettext_noop("Name"),
					  gettext_noop("Owner"),
					  gettext_noop("Validator"));

	if (verbose)
	{
		appendPQExpBuffer(&buf, ",\n  ");
		printACLColumn(&buf, "fdwacl");
		appendPQExpBuffer(&buf,
						  ",\n  fdwoptions AS \"%s\"",
						  gettext_noop("Options"));
	}

	appendPQExpBuffer(&buf, "\nFROM pg_catalog.pg_foreign_data_wrapper\n");

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  NULL, "fdwname", NULL, NULL);

	appendPQExpBuffer(&buf, "ORDER BY 1;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of foreign-data wrappers");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}

/*
 * \des
 *
 * Describes foreign servers.
 */
bool
listForeignServers(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	if (pset.sversion < 80400 && (pset.sversion < 80214 || !isGPDB()))  /* allow for Greenplum 8.2.x */
	{
		fprintf(stderr, _("The server (version %d.%d) does not support foreign servers.\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf,
					  "SELECT s.srvname AS \"%s\",\n"
					  "  pg_catalog.pg_get_userbyid(s.srvowner) AS \"%s\",\n"
					  "  f.fdwname AS \"%s\"",
					  gettext_noop("Name"),
					  gettext_noop("Owner"),
					  gettext_noop("Foreign-data wrapper"));

	if (verbose)
	{
		appendPQExpBuffer(&buf, ",\n  ");
		printACLColumn(&buf, "s.srvacl");
		appendPQExpBuffer(&buf,
						  ",\n"
						  "  s.srvtype AS \"%s\",\n"
						  "  s.srvversion AS \"%s\",\n"
						  "  s.srvoptions AS \"%s\"",
						  gettext_noop("Type"),
						  gettext_noop("Version"),
						  gettext_noop("Options"));
	}

	appendPQExpBuffer(&buf,
					  "\nFROM pg_catalog.pg_foreign_server s\n"
	   "     JOIN pg_catalog.pg_foreign_data_wrapper f ON f.oid=s.srvfdw\n");

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  NULL, "s.srvname", NULL, NULL);

	appendPQExpBuffer(&buf, "ORDER BY 1;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of foreign servers");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}

/*
 * \deu
 *
 * Describes user mappings.
 */
bool
listUserMappings(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	if (pset.sversion < 80400 && (pset.sversion < 80214 || !isGPDB()))  /* allow for Greenplum 8.2.x */
	{
		fprintf(stderr, _("The server (version %d.%d) does not support user mappings.\n"),
				pset.sversion / 10000, (pset.sversion / 100) % 100);
		return true;
	}

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf,
					  "SELECT um.srvname AS \"%s\",\n"
					  "  um.usename AS \"%s\"",
					  gettext_noop("Server"),
					  gettext_noop("User name"));

	if (verbose)
		appendPQExpBuffer(&buf,
						  ",\n  um.umoptions AS \"%s\"",
						  gettext_noop("Options"));

	appendPQExpBuffer(&buf, "\nFROM pg_catalog.pg_user_mappings um\n");

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  NULL, "um.srvname", "um.usename", NULL);

	appendPQExpBuffer(&buf, "ORDER BY 1, 2;");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of user mappings");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);

	PQclear(res);
	return true;
}

/*
 * printACLColumn
 *
 * Helper function for consistently formatting ACL (privilege) columns.
 * The proper targetlist entry is appended to buf.	Note lack of any
 * whitespace or comma decoration.
 */
static void
printACLColumn(PQExpBuffer buf, const char *colname)
{
	if (pset.sversion >= 80100)
		appendPQExpBuffer(buf,
						  "pg_catalog.array_to_string(%s, E'\\n') AS \"%s\"",
						  colname, gettext_noop("Access privileges"));
	else
		appendPQExpBuffer(buf,
						  "pg_catalog.array_to_string(%s, '\\n') AS \"%s\"",
						  colname, gettext_noop("Access privileges"));
}
