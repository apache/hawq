/*-------------------------------------------------------------------------
 *
 * pg_dumpall.c
 *
 * Copyright (c) 2006-2010, Greenplum inc.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * $PostgreSQL: pgsql/src/bin/pg_dump/pg_dumpall.c,v 1.85.2.1 2007/05/15 20:20:24 alvherre Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <time.h>
#include <unistd.h>

#ifdef ENABLE_NLS
#include <locale.h>
#endif

#include "getopt_long.h"

#ifndef HAVE_INT_OPTRESET
int			optreset;
#endif

#include "dumputils.h"
#include "pg_backup.h"


/* version string we expect back from pg_dump */
#define PGDUMP_VERSIONSTR "pg_dump (PostgreSQL) " PG_VERSION "\n"


static const char *progname;

static void help(void);

static void dumpResQueues(PGconn *conn);
static void dumpRoles(PGconn *conn);
static void dumpRoleMembership(PGconn *conn);
static void dumpRoleConstraints(PGconn *conn);
static void dumpFilesystems(PGconn *conn);
static void dumpFilespaces(PGconn *conn);
static void dumpTablespaces(PGconn *conn);
static void dumpCreateDB(PGconn *conn);
static void dumpDatabaseConfig(PGconn *conn, const char *dbname);
static void dumpUserConfig(PGconn *conn, const char *username);
static void makeAlterConfigCommand(PGconn *conn, const char *arrayitem,
					   const char *type, const char *name);
static void dumpDatabases(PGconn *conn);
static void dumpTimestamp(char *msg);

static int	runPgDump(const char *dbname);
static PGconn *connectDatabase(const char *dbname, const char *pghost, const char *pgport,
	  const char *pguser, enum trivalue prompt_password, bool fail_on_error);
static PGresult *executeQuery(PGconn *conn, const char *query);
static void executeCommand(PGconn *conn, const char *query);

static char pg_dump_bin[MAXPGPATH];
static PQExpBuffer pgdumpopts;
static bool output_clean = false;
static bool skip_acls = false;
static bool verbose = false;
static bool ignoreVersion = false;
static bool resource_queues = false;
static bool filespaces = false;

static int	disable_dollar_quoting = 0;
static int	disable_triggers = 0;
static int	use_setsessauth = 0;
static int	server_version;

static FILE *OPF;
static char *filename = NULL;

int
main(int argc, char *argv[])
{
	char	   *pghost = NULL;
	char	   *pgport = NULL;
	char	   *pguser = NULL;
	char	   *pgdb = NULL;

	enum trivalue prompt_password = TRI_DEFAULT;
	bool		data_only = false;
	bool		globals_only = false;
	bool		schema_only = false;
	static int	gp_migrator = 0;
	bool		gp_syntax = false;
	bool		no_gp_syntax = false;
	PGconn	   *conn;
	int			encoding;
	const char *std_strings;
	int			c,
				ret;

	static struct option long_options[] = {
		{"data-only", no_argument, NULL, 'a'},
		{"clean", no_argument, NULL, 'c'},
		{"inserts", no_argument, NULL, 'd'},
		{"attribute-inserts", no_argument, NULL, 'D'},
		{"column-inserts", no_argument, NULL, 'D'},
		{"file", required_argument, NULL, 'f'},
		{"globals-only", no_argument, NULL, 'g'},
		{"host", required_argument, NULL, 'h'},
		{"ignore-version", no_argument, NULL, 'i'},
		{"database", required_argument, NULL, 'l'},
		{"oids", no_argument, NULL, 'o'},
		{"no-owner", no_argument, NULL, 'O'},
		{"port", required_argument, NULL, 'p'},
		{"schema-only", no_argument, NULL, 's'},
		{"superuser", required_argument, NULL, 'S'},
		{"username", required_argument, NULL, 'U'},
		{"verbose", no_argument, NULL, 'v'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"no-privileges", no_argument, NULL, 'x'},
		{"no-acl", no_argument, NULL, 'x'},
		{"resource-queues", no_argument, NULL, 'r'},
		{"filespaces", no_argument, NULL, 'F'},

		/*
		 * the following options don't have an equivalent short option letter
		 */
		{"disable-dollar-quoting", no_argument, &disable_dollar_quoting, 1},
		{"disable-triggers", no_argument, &disable_triggers, 1},
		{"use-set-session-authorization", no_argument, &use_setsessauth, 1},

		/* START MPP ADDITION */
		{"gp-syntax", no_argument, NULL, 1},
		{"no-gp-syntax", no_argument, NULL, 2},
		{"gp-migrator", no_argument, &gp_migrator, 1},
		/* END MPP ADDITION */

		{NULL, 0, NULL, 0}
	};

	int			optindex;

	set_pglocale_pgservice(argv[0], "pg_dump");

	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			help();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_dumpall (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	if ((ret = find_other_exec(argv[0], "pg_dump", PGDUMP_VERSIONSTR,
							   pg_dump_bin)) < 0)
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(argv[0], full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (ret == -1)
			fprintf(stderr,
					_("The program \"pg_dump\" is needed by %s "
					  "but was not found in the\n"
					  "same directory as \"%s\".\n"
					  "Check your installation.\n"),
					progname, full_path);
		else
			fprintf(stderr,
					_("The program \"pg_dump\" was found by \"%s\"\n"
					  "but was not the same version as %s.\n"
					  "Check your installation.\n"),
					full_path, progname);
		exit(1);
	}

	pgdumpopts = createPQExpBuffer();

	while ((c = getopt_long(argc, argv, "acdDf:Fgh:il:oOp:rsS:U:vwWxX:", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'a':
				data_only = true;
				appendPQExpBuffer(pgdumpopts, " -a");
				break;

			case 'c':
				output_clean = true;
				break;

			case 'd':
			case 'D':
				appendPQExpBuffer(pgdumpopts, " -%c", c);
				break;

			case 'f':
				filename = optarg;
#ifndef WIN32
				appendPQExpBuffer(pgdumpopts, " -f '%s'", filename);
#else
				appendPQExpBuffer(pgdumpopts, " -f \"%s\"", filename);
#endif

				break;

			case 'g':
				globals_only = true;
				break;

			case 'h':
				pghost = optarg;
#ifndef WIN32
				appendPQExpBuffer(pgdumpopts, " -h '%s'", pghost);
#else
				appendPQExpBuffer(pgdumpopts, " -h \"%s\"", pghost);
#endif

				break;

			case 'i':
				/* ignored, deprecated option */
				break;


			case 'l':
				pgdb = optarg;
				break;

			case 'o':
				appendPQExpBuffer(pgdumpopts, " -o");
				break;

			case 'O':
				appendPQExpBuffer(pgdumpopts, " -O");
				break;

			case 'p':
				pgport = optarg;
#ifndef WIN32
				appendPQExpBuffer(pgdumpopts, " -p '%s'", pgport);
#else
				appendPQExpBuffer(pgdumpopts, " -p \"%s\"", pgport);
#endif
				break;

			case 'r':
				resource_queues = true;
				break;

			case 'F':
				filespaces = true;
				break;

			case 's':
				schema_only = true;
				appendPQExpBuffer(pgdumpopts, " -s");
				break;

			case 'S':
#ifndef WIN32
				appendPQExpBuffer(pgdumpopts, " -S '%s'", optarg);
#else
				appendPQExpBuffer(pgdumpopts, " -S \"%s\"", optarg);
#endif
				break;

			case 'U':
				pguser = optarg;
#ifndef WIN32
				appendPQExpBuffer(pgdumpopts, " -U '%s'", pguser);
#else
				appendPQExpBuffer(pgdumpopts, " -U \"%s\"", pguser);
#endif
				break;

			case 'v':
				verbose = true;
				appendPQExpBuffer(pgdumpopts, " -v");
				break;

			case 'w':
				prompt_password = TRI_NO;
				appendPQExpBuffer(pgdumpopts, " -w");
				break;

			case 'W':
				prompt_password = TRI_YES;
				appendPQExpBuffer(pgdumpopts, " -W");
				break;

			case 'x':
				skip_acls = true;
				appendPQExpBuffer(pgdumpopts, " -x");
				break;

			case 'X':
				/* -X is a deprecated alternative to long options */
				if (strcmp(optarg, "disable-dollar-quoting") == 0)
					appendPQExpBuffer(pgdumpopts, " --disable-dollar-quoting");
				else if (strcmp(optarg, "disable-triggers") == 0)
					appendPQExpBuffer(pgdumpopts, " --disable-triggers");
				else if (strcmp(optarg, "use-set-session-authorization") == 0)
					 /* no-op, still allowed for compatibility */ ;
				else
				{
					fprintf(stderr,
							_("%s: invalid -X option -- %s\n"),
							progname, optarg);
					fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
					exit(1);
				}
				break;

			case 0:
				break;

				/* START MPP ADDITION */
			case 1:
				/* gp-format */
				appendPQExpBuffer(pgdumpopts, " --gp-syntax");
				gp_syntax = true;
				resource_queues = true; /* -r is implied by --gp-syntax */
				break;
			case 2:
				/* no-gp-format */
				appendPQExpBuffer(pgdumpopts, " --no-gp-syntax");
				no_gp_syntax = true;
				break;

				/* END MPP ADDITION */

			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

	/* Add long options to the pg_dump argument list */
	if (disable_dollar_quoting)
		appendPQExpBuffer(pgdumpopts, " --disable-dollar-quoting");
	if (disable_triggers)
		appendPQExpBuffer(pgdumpopts, " --disable-triggers");
	if (use_setsessauth)
		appendPQExpBuffer(pgdumpopts, " --use-set-session-authorization");

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (gp_syntax && no_gp_syntax)
	{
		fprintf(stderr, _("%s: options \"--gp-syntax\" and \"--no-gp-syntax\" cannot be used together\n"),
				progname);
		exit(1);
	}

	/*
	 * If there was a database specified on the command line, use that,
	 * otherwise try to connect to database "postgres", and failing that
	 * "template1".  "postgres" is the preferred choice for 8.1 and later
	 * servers, but it usually will not exist on older ones.
	 */
	if (pgdb)
	{
		conn = connectDatabase(pgdb, pghost, pgport, pguser,
							   prompt_password, false);

		if (!conn)
		{
			fprintf(stderr, _("%s: could not connect to database \"%s\"\n"),
					progname, pgdb);
			exit(1);
		}
	}
	else
	{
		conn = connectDatabase("postgres", pghost, pgport, pguser,
							   prompt_password, false);
		if (!conn)
			conn = connectDatabase("template1", pghost, pgport, pguser,
								   prompt_password, true);

		if (!conn)
		{
			fprintf(stderr, _("%s: could not connect to databases \"postgres\" or \"template1\"\n"
							  "Please specify an alternative database.\n"),
					progname);
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
			exit(1);
		}
	}

	/*
	 * Open the output file if required, otherwise use stdout
	 */
	if (filename)
	{
		OPF = fopen(filename, PG_BINARY_W);
		if (!OPF)
		{
			fprintf(stderr, _("%s: could not open the output file \"%s\": %s\n"),
					progname, filename, strerror(errno));
			exit(1);
		}
	}
	else
		OPF = stdout;

	/*
	 * Get the active encoding and the standard_conforming_strings setting, so
	 * we know how to escape strings.
	 */
	encoding = PQclientEncoding(conn);
	std_strings = PQparameterStatus(conn, "standard_conforming_strings");
	if (!std_strings)
		std_strings = "off";

	fprintf(OPF,"--\n-- Greenplum Database cluster dump\n--\n\n");
	if (verbose)
		dumpTimestamp("Started on");

	fprintf(OPF, "\\connect postgres\n\n");

	if (!data_only)
	{
		/* Replicate encoding and std_strings in output */
		fprintf(OPF, "SET client_encoding = '%s';\n",
			   pg_encoding_to_char(encoding));
		fprintf(OPF, "SET standard_conforming_strings = %s;\n", std_strings);
		if (strcmp(std_strings, "off") == 0)
			fprintf(OPF, "SET escape_string_warning = 'off';\n");

		fprintf(OPF, "SET gp_called_by_pgdump = true;\n");
		fprintf(OPF, "\n");

		/* Dump Resource Queues */
		if (resource_queues)
			dumpResQueues(conn);

		/* Dump roles (users) */
		dumpRoles(conn);

		/* Dump role memberships */
		dumpRoleMembership(conn);

		/* Dump role constraints */
		dumpRoleConstraints(conn);

		/* Dump filesystems */
		if (filespaces)
			dumpFilesystems(conn);

		/* Dump filespaces */
		if (filespaces)
			dumpFilespaces(conn);

		/* Dump tablespaces */
		dumpTablespaces(conn);

		/* Dump CREATE DATABASE commands */
		if (!globals_only)
			dumpCreateDB(conn);
	}

	if (!globals_only)
		dumpDatabases(conn);

	PQfinish(conn);

	if (verbose)
		dumpTimestamp("Completed on");
	fprintf(OPF, "--\n-- PostgreSQL database cluster dump complete\n--\n\n");

	if (filename)
		fclose(OPF);

	exit(0);
}



static void
help(void)
{
	printf(_("%s extracts a PostgreSQL database cluster into an SQL script file.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);

	printf(_("\nGeneral options:\n"));
	printf(_("  -f, --file=FILENAME      output file name\n"));
	printf(_("  -i, --ignore-version     proceed even when server version mismatches\n"
			 "                           pg_dumpall version\n"));
	printf(_("  --help                   show this help, then exit\n"));
	printf(_("  --version                output version information, then exit\n"));
	printf(_("\nOptions controlling the output content:\n"));
	printf(_("  -a, --data-only          dump only the data, not the schema\n"));
	printf(_("  -c, --clean              clean (drop) databases before recreating\n"));
	printf(_("  -d, --inserts            dump data as INSERT, rather than COPY, commands\n"));
	printf(_("  -D, --column-inserts     dump data as INSERT commands with column names\n"));
	printf(_("  -F, --filespaces         dump filespace data\n"));
	printf(_("  -g, --globals-only       dump only global objects, no databases\n"));
	printf(_("  -o, --oids               include OIDs in dump\n"));
	printf(_("  -O, --no-owner           skip restoration of object ownership\n"));
	printf(_("  -r, --resource-queues    dump resource queue data\n"));
	printf(_("  -s, --schema-only        dump only the schema, no data\n"));
	printf(_("  -S, --superuser=NAME     specify the superuser user name to use in the dump\n"));
	printf(_("  -x, --no-privileges      do not dump privileges (grant/revoke)\n"));
	printf(_("  --disable-dollar-quoting\n"
			 "                           disable dollar quoting, use SQL standard quoting\n"));
	printf(_("  --disable-triggers       disable triggers during data-only restore\n"));
	printf(_("  --use-set-session-authorization\n"
			 "                           use SESSION AUTHORIZATION commands instead of\n"
			 "                           OWNER TO commands\n"));
	/* START MPP ADDITION */
	printf(_("  --gp-syntax              dump with Greenplum Database syntax (default if gpdb)\n"));
	printf(_("  --no-gp-syntax           dump without Greenplum Database syntax (default if postgresql)\n"));
	/* END MPP ADDITION */

	printf(_("\nConnection options:\n"));
	printf(_("  -h, --host=HOSTNAME      database server host or socket directory\n"));
	printf(_("  -l, --database=DBNAME    alternative default database\n"));
	printf(_("  -p, --port=PORT          database server port number\n"));
	printf(_("  -U, --username=NAME      connect as specified database user\n"));
	printf(_("  -w, --no-password        never prompt for password\n"));
	printf(_("  -W, --password           force password prompt (should happen automatically)\n"));

	printf(_("\nIf -f/--file is not used, then the SQL script will be written to the standard\n"
			 "output.\n\n"));
	printf(_("Report bugs to <pgsql-bugs@postgresql.org>.\n"));
}


/*
 * Dump resource queues
 */
static void
dumpResQueues(PGconn *conn)
{
	PQExpBuffer buf = createPQExpBuffer();
	PQExpBuffer sql = createPQExpBuffer();
	PGresult   *res;
	PGresult   *res2;
	int			i_rsqname,
				i_resname,
				i_ressetting;
	int			i;
	char	   *prev_rsqname = NULL;
	bool		bWith = false;

	printfPQExpBuffer(buf,
					  "SELECT oid, rsqname, 'parent' as resname, "
					  "parentoid::text as ressetting, "
					  "1 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'active_statements' as resname, "
					  "activestats::text as ressetting, "
					  "2 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'memory_limit_cluster' as resname, "
					  "memorylimit::text as ressetting, "
					  "3 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'core_limit_cluster' as resname, "
					  "corelimit::text as ressetting, "
					  "4 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'resource_overcommit_factor' as resname, "
					  "resovercommit::text as ressetting, "
					  "5 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'allocation_policy' as resname, "
					  "allocpolicy::text as ressetting, "
					  "6 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'vseg_resource_quota' as resname, "
					  "vsegresourcequota::text as ressetting, "
					  "7 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'nvseg_upper_limit' as resname, "
					  "nvsegupperlimit::text as ressetting, "
					  "8 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'nvseg_lower_limit' as resname, "
					  "nvseglowerlimit::text as ressetting, "
					  "9 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'nvseg_upper_limit_perseg' as resname, "
					  "nvsegupperlimitperseg::text as ressetting, "
					  "10 as ord FROM pg_resqueue "
					  "UNION "
					  "SELECT oid, rsqname, 'nvseg_lower_limit_perseg' as resname, "
					  "nvseglowerlimitperseg::text as ressetting, "
					  "11 as ord FROM pg_resqueue "
					  "order by oid, ord"
		);

	res = executeQuery(conn, buf->data);

	i_rsqname = PQfnumber(res, "rsqname");
	i_resname = PQfnumber(res, "resname");
	i_ressetting = PQfnumber(res, "ressetting");

	if (PQntuples(res) > 0)
	    fprintf(OPF, "--\n-- Resource Queues\n--\n\n");

	/*
	 * settings for resource queue are spread over multiple rows, but sorted
	 * by queue oid :
	 *
	 * oid | rsqname   |     resname     | ressetting | ord
	 * ----+-----------+-----------------+------------+-----
	 *
	 * This format lets us support an arbitrary number of capability
	 * entries.  So watch for change of rsqname to switch to next CREATE
	 * statement.
	 *
	 * We order rows based on oid values, because we would like to let resource
	 * queues be created from the ones having smaller oid values, which guarantees
	 * that every queue can find its parent queue.
	 *
	 */

	for (i = 0; i < PQntuples(res); i++)
	{
		const char *rsqname		= NULL;
		const char *resname		= NULL;
		const char *ressetting	= NULL;

		rsqname    = PQgetvalue(res, i, i_rsqname);
		resname    = PQgetvalue(res, i, i_resname);
		ressetting = PQgetvalue(res, i, i_ressetting);

		/* skip pg_root */
		if (0 == strcmp(rsqname, "pg_root"))
			continue;

		/* if first CREATE statement, or name changed... */
		if (!prev_rsqname || (0 != strcmp(rsqname, prev_rsqname)))
		{
			if (prev_rsqname)
			{
				/* terminate the WITH if necessary */
				if (bWith)
					appendPQExpBuffer(buf, ") ");

				appendPQExpBuffer(buf, ";\n");

				fprintf(OPF, "%s", buf->data);

				free(prev_rsqname);
			}

			bWith = false;

			/* save the name */
			prev_rsqname = strdup(rsqname);

			resetPQExpBuffer(buf);

			/* MPP-6926: cannot DROP or CREATE default queue, so ALTER it  */
			if (0 == strcmp(rsqname, "pg_default"))
				appendPQExpBuffer(buf, "ALTER RESOURCE QUEUE %s", fmtId(rsqname));
			else
				appendPQExpBuffer(buf, "CREATE RESOURCE QUEUE %s", fmtId(rsqname));
		}

		/* don't update pg_default's parent */
		if (0 == strcmp(rsqname, "pg_default") && 0 == strcmp(resname, "parent"))
			continue;

		/* build the WITH clause */
		if (bWith)
			appendPQExpBuffer(buf, ",\n");
		else
		{
			bWith = true;
			appendPQExpBuffer(buf, "\n WITH (");
		}

		if (0 == strcmp("active_statements",          resname) ||
			0 == strcmp("resource_overcommit_factor", resname) ||
			0 == strcmp("nvseg_upper_limit",          resname) ||
			0 == strcmp("nvseg_lower_limit",          resname) ||
			0 == strcmp("nvseg_upper_limit_perseg",   resname) ||
			0 == strcmp("nvseg_lower_limit_perseg",   resname))
			/* numeric */
			appendPQExpBuffer(buf, " %s=%s", resname, ressetting);
		else if (0 == strcmp("parent", resname))
		{
			/* find parent's name with oid. */
			appendPQExpBuffer(sql, "SELECT rsqname FROM pg_resqueue WHERE oid='%s'", ressetting);
			res2 = executeQuery(conn, sql->data);
			char *parent = PQgetvalue(res2, 0, 0);
			appendPQExpBuffer(buf, " %s='%s'", resname, parent);
			resetPQExpBuffer(sql);
			PQclear(res2);
		}
		else
			/* string */
			appendPQExpBuffer(buf, " %s='%s'", resname, ressetting);

	}							/* end for */

	/* need to write out last statement */
	if (prev_rsqname)
	{
		/* terminate the WITH if necessary */
		if (bWith)
			appendPQExpBuffer(buf, ") ");

		appendPQExpBuffer(buf, ";\n");

		fprintf(OPF, "%s", buf->data);

		free(prev_rsqname);
	}

	PQclear(res);

	fprintf(OPF, "\n\n");
}


/*
 * Dump roles
 */
static void
dumpRoles(PGconn *conn)
{
	PQExpBuffer buf = createPQExpBuffer();
	PGresult   *res;
	int			i_rolname,
				i_rolsuper,
				i_rolinherit,
				i_rolcreaterole,
				i_rolcreatedb,
				i_rolcatupdate,
				i_rolcanlogin,
				i_rolconnlimit,
				i_rolpassword,
				i_rolvaliduntil,
				i_rolcomment,
				i_rolqueuename = -1,	/* keep compiler quiet */
				i_rolcreaterextgpfd = -1,
				i_rolcreaterexthttp = -1,
				i_rolcreatewextgpfd = -1,
				i_rolcreaterexthdfs = -1,
				i_rolcreatewexthdfs = -1;
	int			i;
	bool		exttab_auth = (server_version >= 80214);
	bool		hdfs_auth = (server_version >= 80215);
	char	   *resq_col = resource_queues ? ", (SELECT rsqname FROM pg_resqueue WHERE "
	"  pg_resqueue.oid = rolresqueue) AS rolqueuename " : "";
	char	   *extauth_col = exttab_auth ? ", rolcreaterextgpfd, rolcreaterexthttp, rolcreatewextgpfd" : "";
	char	   *hdfs_col = hdfs_auth ? ", rolcreaterexthdfs, rolcreatewexthdfs " : "";

	/*
	 * Query to select role info get resqueue if version support it get
	 * external table auth on gpfdist, gpfdists and http if version support it get
	 * external table auth on gphdfs if version support it note: rolconfig is
	 * dumped later
	 */
	printfPQExpBuffer(buf,
					  "SELECT rolname, rolsuper, rolinherit, "
					  "rolcreaterole, rolcreatedb, rolcatupdate, "
					  "rolcanlogin, rolconnlimit, rolpassword, "
					  "rolvaliduntil, "
					  "pg_catalog.shobj_description(oid, 'pg_authid') as rolcomment "
					  " %s %s %s"
					  "FROM pg_authid "
					  "ORDER BY 1",
					  resq_col, extauth_col, hdfs_col);

	res = executeQuery(conn, buf->data);

	i_rolname = PQfnumber(res, "rolname");
	i_rolsuper = PQfnumber(res, "rolsuper");
	i_rolinherit = PQfnumber(res, "rolinherit");
	i_rolcreaterole = PQfnumber(res, "rolcreaterole");
	i_rolcreatedb = PQfnumber(res, "rolcreatedb");
	i_rolcatupdate = PQfnumber(res, "rolcatupdate");
	i_rolcanlogin = PQfnumber(res, "rolcanlogin");
	i_rolconnlimit = PQfnumber(res, "rolconnlimit");
	i_rolpassword = PQfnumber(res, "rolpassword");
	i_rolvaliduntil = PQfnumber(res, "rolvaliduntil");
	i_rolcomment = PQfnumber(res, "rolcomment");

	if (resource_queues)
		i_rolqueuename = PQfnumber(res, "rolqueuename");

	if (exttab_auth)
	{
		i_rolcreaterextgpfd = PQfnumber(res, "rolcreaterextgpfd");
		i_rolcreaterexthttp = PQfnumber(res, "rolcreaterexthttp");
		i_rolcreatewextgpfd = PQfnumber(res, "rolcreatewextgpfd");
		if (hdfs_auth)
		{
			i_rolcreaterexthdfs = PQfnumber(res, "rolcreaterexthdfs");
			i_rolcreatewexthdfs = PQfnumber(res, "rolcreatewexthdfs");
		}
	}

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Roles\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		const char *rolename;

		rolename = PQgetvalue(res, i, i_rolname);

		resetPQExpBuffer(buf);

		if (output_clean)
			appendPQExpBuffer(buf, "DROP ROLE %s;\n", fmtId(rolename));

		/*
		 * We dump CREATE ROLE followed by ALTER ROLE to ensure that the role
		 * will acquire the right properties even if it already exists. (The
		 * above DROP may therefore seem redundant, but it isn't really,
		 * because this technique doesn't get rid of role memberships.)
		 */
		appendPQExpBuffer(buf, "CREATE ROLE %s;\n", fmtId(rolename));
		appendPQExpBuffer(buf, "ALTER ROLE %s WITH", fmtId(rolename));

		if (strcmp(PQgetvalue(res, i, i_rolsuper), "t") == 0)
			appendPQExpBuffer(buf, " SUPERUSER");
		else
			appendPQExpBuffer(buf, " NOSUPERUSER");

		if (strcmp(PQgetvalue(res, i, i_rolinherit), "t") == 0)
			appendPQExpBuffer(buf, " INHERIT");
		else
			appendPQExpBuffer(buf, " NOINHERIT");

		if (strcmp(PQgetvalue(res, i, i_rolcreaterole), "t") == 0)
			appendPQExpBuffer(buf, " CREATEROLE");
		else
			appendPQExpBuffer(buf, " NOCREATEROLE");

		if (strcmp(PQgetvalue(res, i, i_rolcreatedb), "t") == 0)
			appendPQExpBuffer(buf, " CREATEDB");
		else
			appendPQExpBuffer(buf, " NOCREATEDB");

		if (strcmp(PQgetvalue(res, i, i_rolcanlogin), "t") == 0)
			appendPQExpBuffer(buf, " LOGIN");
		else
			appendPQExpBuffer(buf, " NOLOGIN");

		if (strcmp(PQgetvalue(res, i, i_rolconnlimit), "-1") != 0)
			appendPQExpBuffer(buf, " CONNECTION LIMIT %s",
							  PQgetvalue(res, i, i_rolconnlimit));

		if (!PQgetisnull(res, i, i_rolpassword))
		{
			appendPQExpBuffer(buf, " PASSWORD ");
			appendStringLiteralConn(buf, PQgetvalue(res, i, i_rolpassword), conn);
		}

		if (!PQgetisnull(res, i, i_rolvaliduntil))
			appendPQExpBuffer(buf, " VALID UNTIL '%s'",
							  PQgetvalue(res, i, i_rolvaliduntil));

		if (resource_queues)
		{
			if (!PQgetisnull(res, i, i_rolqueuename))
				appendPQExpBuffer(buf, " RESOURCE QUEUE %s",
								  PQgetvalue(res, i, i_rolqueuename));
		}

		if (exttab_auth)
		{
			/* we use the same privilege for gpfdist and gpfdists */
			if (!PQgetisnull(res, i, i_rolcreaterextgpfd) &&
				strcmp(PQgetvalue(res, i, i_rolcreaterextgpfd), "t") == 0)
				appendPQExpBuffer(buf, " CREATEEXTTABLE (protocol='gpfdist', type='readable')");

			if (!PQgetisnull(res, i, i_rolcreatewextgpfd) &&
				strcmp(PQgetvalue(res, i, i_rolcreatewextgpfd), "t") == 0)
				appendPQExpBuffer(buf, " CREATEEXTTABLE (protocol='gpfdist', type='writable')");

			if (!PQgetisnull(res, i, i_rolcreaterexthttp) &&
				strcmp(PQgetvalue(res, i, i_rolcreaterexthttp), "t") == 0)
				appendPQExpBuffer(buf, " CREATEEXTTABLE (protocol='http')");

			if (hdfs_auth)
			{
				if (!PQgetisnull(res, i, i_rolcreaterexthdfs) &&
					strcmp(PQgetvalue(res, i, i_rolcreaterexthdfs), "t") == 0)
					appendPQExpBuffer(buf, " CREATEEXTTABLE (protocol='gphdfs', type='readable')");

				if (!PQgetisnull(res, i, i_rolcreatewexthdfs) &&
					strcmp(PQgetvalue(res, i, i_rolcreatewexthdfs), "t") == 0)
					appendPQExpBuffer(buf, " CREATEEXTTABLE (protocol='gphdfs', type='writable')");
			}
		}

		appendPQExpBuffer(buf, ";\n");

		if (!PQgetisnull(res, i, i_rolcomment))
		{
			appendPQExpBuffer(buf, "COMMENT ON ROLE %s IS ", fmtId(rolename));
			appendStringLiteralConn(buf, PQgetvalue(res, i, i_rolcomment), conn);
			appendPQExpBuffer(buf, ";\n");
		}

		fprintf(OPF, "%s", buf->data);

		dumpUserConfig(conn, rolename);
	}

	PQclear(res);

	fprintf(OPF, "\n\n");

	destroyPQExpBuffer(buf);
}


/*
 * Dump role memberships.  This code is used for 8.1 and later servers.
 *
 * Note: we expect dumpRoles already created all the roles, but there is
 * no membership yet.
 */
static void
dumpRoleMembership(PGconn *conn)
{
	PGresult   *res;
	int			i;

	res = executeQuery(conn, "SELECT ur.rolname AS roleid, "
					   "um.rolname AS member, "
					   "a.admin_option, "
					   "ug.rolname AS grantor "
					   "FROM pg_auth_members a "
					   "LEFT JOIN pg_authid ur on ur.oid = a.roleid "
					   "LEFT JOIN pg_authid um on um.oid = a.member "
					   "LEFT JOIN pg_authid ug on ug.oid = a.grantor "
					   "ORDER BY 1,2,3");

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Role memberships\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *roleid = PQgetvalue(res, i, 0);
		char	   *member = PQgetvalue(res, i, 1);
		char	   *option = PQgetvalue(res, i, 2);

		fprintf(OPF, "GRANT %s", fmtId(roleid));
		fprintf(OPF, " TO %s", fmtId(member));
		if (*option == 't')
			fprintf(OPF, " WITH ADMIN OPTION");

		/*
		 * We don't track the grantor very carefully in the backend, so cope
		 * with the possibility that it has been dropped.
		 */
		if (!PQgetisnull(res, i, 3))
		{
			char	   *grantor = PQgetvalue(res, i, 3);

			fprintf(OPF, " GRANTED BY %s", fmtId(grantor));
		}
		fprintf(OPF, ";\n");
	}

	PQclear(res);

	fprintf(OPF, "\n\n");
}

/*
 * Dump role time constraints. 
 *
 * Note: we expect dumpRoles already created all the roles, but there are
 * no time constraints yet.
 */
static void
dumpRoleConstraints(PGconn *conn)
{
	PGresult   *res;
	int 		i;

	res = executeQuery(conn, "SELECT a.rolname, c.start_day, c.start_time, c.end_day, c.end_time "
							 "FROM pg_authid a, pg_auth_time_constraint c "
							 "WHERE a.oid = c.authid "
							 "ORDER BY 1");

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Role time constraints\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		char		*rolname 	= PQgetvalue(res, i, 0);
		char		*start_day 	= PQgetvalue(res, i, 1);
		char 		*start_time = PQgetvalue(res, i, 2);
		char		*end_day 	= PQgetvalue(res, i, 3);
		char 		*end_time 	= PQgetvalue(res, i, 4);

		fprintf(OPF, "ALTER ROLE %s DENY BETWEEN DAY %s TIME '%s' AND DAY %s TIME '%s';\n", 
				fmtId(rolname), start_day, start_time, end_day, end_time);
	}

	PQclear(res);

	fprintf(OPF, "\n\n");
}

/*
 * Dump filesystems
 */
static void
dumpFilesystems(PGconn *conn)
{
	int			i;
	PGresult   *res;

	/*
	 * Get all filesystems execpt built-in ones (named pg_xxx)
	 */
	if (server_version < 80214)
	{
		/* Filespaces were introduced in GP 4.0 (server_version 8.2.14) */
		return;
	}
	else
	{
		res = executeQuery(conn, "SELECT fsysname, "
						   "pg_catalog.pg_get_userbyid(fsysowner) as fsysowner, "
						   "pg_catalog.shobj_description(oid, 'pg_filesystem'), "
						   "fsyslibfile, "
    					   "fsysconnfn, "
    					   "fsysdisconnfn,"
    					   "fsysopenfn, "
    					   "fsysclosefn, "
    					   "fsysseekfn, "
    					   "fsystellfn, "
    					   "fsysreadfn, "
    					   "fsyswritefn, "
    					   "fsysflushfn, "
    					   "fsysdeletefn, "
    					   "fsyschmodfn, "
    					   "fsysmkdirfn, "
    					   "fsystruncatefn, "
    					   "fsysgetpathinfofn, "
    					   "fsysfreefileinfofn "
						   "FROM pg_catalog.pg_filesystem "
						   "WHERE fsysname !~ '^hdfs$' "
						   "ORDER BY 1");
	}

	if (PQntuples(res) > 0)
			fprintf(OPF, "--\n-- Filesystems\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		PQExpBuffer buf = createPQExpBuffer();
		char	   *fsysname = PQgetvalue(res, i, 0);
		/*
		 * TODO: filesystem owner is not supported right now.
		 * char	   *fsysowner = PQgetvalue(res, i, 1);
		 */
		char	   *fsysdesc = PQgetvalue(res, i, 2);
		char	   *fsyslibfile = PQgetvalue(res, i, 3);
		char	   *fsysconnfn = PQgetvalue(res, i, 4);
		char	   *fsysdisconnfn = PQgetvalue(res, i, 5);
		char	   *fsysopenfn = PQgetvalue(res, i, 6);
		char	   *fsysclosefn = PQgetvalue(res, i, 7);
		char	   *fsysseekfn = PQgetvalue(res, i, 8);
		char	   *fsystellfn = PQgetvalue(res, i, 9);
		char	   *fsysreadfn = PQgetvalue(res, i, 10);
		char	   *fsyswritefn = PQgetvalue(res, i, 11);
		char	   *fsysflushfn = PQgetvalue(res, i, 12);
		char	   *fsysdeletefn = PQgetvalue(res, i, 13);
		char	   *fsyschmodfn = PQgetvalue(res, i, 14);
		char	   *fsysmkdirfn = PQgetvalue(res, i, 15);
		char	   *fsystruncatefn = PQgetvalue(res, i, 16);
		char	   *fsysgetpathinfofn = PQgetvalue(res, i, 17);
		char	   *fsysfreefileinfofn = PQgetvalue(res, i, 18);

		/* quote name if needed */
		fsysname = strdup(fmtId(fsysname));

		/*
		 * Drop existing filespace if required.
		 *
		 * Note: this statement will fail if the an existing filespace is not
		 * empty.  But this is no different from the related code in the rest
		 * of pg_dump.
		 */
		if (output_clean)
			appendPQExpBuffer(buf, "DROP FILESYSTEM %s;\n", fsysname);

		/* Begin creating the filespace definition */
		appendPQExpBuffer(buf, "CREATE FILESYSTEM %s", fsysname);
		/* TODO: owern is not support right now. */
		appendPQExpBuffer(buf, " (\n");

		appendPQExpBuffer(buf, "gpfs_libfile = \"%s\",\n", fsyslibfile);
		appendPQExpBuffer(buf, "gpfs_connect = \"%s\",\n", fsysconnfn);
		appendPQExpBuffer(buf, "gpfs_disconnect = \"%s\",\n", fsysdisconnfn);
		appendPQExpBuffer(buf, "gpfs_open = \"%s\",\n", fsysopenfn);
		appendPQExpBuffer(buf, "gpfs_close = \"%s\",\n", fsysclosefn);
		appendPQExpBuffer(buf, "gpfs_seek = \"%s\",\n", fsysseekfn);
		appendPQExpBuffer(buf, "gpfs_tell = \"%s\",\n", fsystellfn);
		appendPQExpBuffer(buf, "gpfs_read = \"%s\",\n", fsysreadfn);
		appendPQExpBuffer(buf, "gpfs_write = \"%s\",\n", fsyswritefn);
		appendPQExpBuffer(buf, "gpfs_flush = \"%s\",\n", fsysflushfn);
		appendPQExpBuffer(buf, "gpfs_delete = \"%s\",\n", fsysdeletefn);
		appendPQExpBuffer(buf, "gpfs_chmod = \"%s\",\n", fsyschmodfn);
		appendPQExpBuffer(buf, "gpfs_mkdir = \"%s\",\n", fsysmkdirfn);
		appendPQExpBuffer(buf, "gpfs_truncate = \"%s\",\n", fsystruncatefn);
		appendPQExpBuffer(buf, "gpfs_getpathinfo = \"%s\",\n", fsysgetpathinfofn);
		appendPQExpBuffer(buf, "gpfs_freefileinfo = \"%s\"\n", fsysfreefileinfofn);

		/* Finnish off the statement */
		appendPQExpBuffer(buf, "\n);\n");

		/* Add a comment on the filespace if specified */
		if (fsysdesc && strlen(fsysdesc))
		{
			appendPQExpBuffer(buf, "COMMENT ON FILESYSTEM %s IS ", fsysname);
			appendStringLiteralConn(buf, fsysdesc, conn);
			appendPQExpBuffer(buf, ";\n");
		}

		/* Output the results */
		fprintf(OPF, "%s", buf->data);

		free(fsysname);
		destroyPQExpBuffer(buf);
	}

	PQclear(res);
	fprintf(OPF, "\n\n");
}

/*
 * Dump filespaces.
 */
static void
dumpFilespaces(PGconn *conn)
{
	int			i,
				j;
	PGresult   *res;

	/*
	 * Get all filespaces execpt built-in ones (named pg_xxx)
	 */
	if (server_version < 80214)
	{
		/* Filespaces were introduced in GP 4.0 (server_version 8.2.14) */
		return;
	}
	else
	{
		res = executeQuery(conn, "SELECT fsname, oid, "
						   "pg_catalog.pg_get_userbyid(fsowner) as fsowner, "
						   "pg_catalog.shobj_description(oid, 'pg_filespace'), "
						   "(select fsysname from pg_catalog.pg_filesystem where oid = fsfsys) as fsfsys "
						   "FROM pg_catalog.pg_filespace "
						   "WHERE fsname !~ '^pg_' "
						   "ORDER BY 1");
	}

	if (PQntuples(res) > 0)
			fprintf(OPF, "--\n-- Filespaces\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		PQExpBuffer buf = createPQExpBuffer();
		char	   *fsname = PQgetvalue(res, i, 0);
		char	   *fsoid = PQgetvalue(res, i, 1);
		char	   *fsowner = PQgetvalue(res, i, 2);
		char	   *fsdesc = PQgetvalue(res, i, 3);
		bool		fsfsys_is_local = PQgetisnull(res, i, 4);
		char	   *fsfsys = PQgetvalue(res, i, 4);
		PQExpBuffer subbuf;
		PGresult   *entries = NULL;

		/* quote name if needed */
		fsname = strdup(fmtId(fsname));

		/*
		 * Drop existing filespace if required.
		 *
		 * Note: this statement will fail if the an existing filespace is not
		 * empty.  But this is no different from the related code in the rest
		 * of pg_dump.
		 */
		if (output_clean)
			appendPQExpBuffer(buf, "DROP FILESPACE %s;\n", fsname);

		/* Begin creating the filespace definition */
		appendPQExpBuffer(buf, "CREATE FILESPACE %s", fsname);
		appendPQExpBuffer(buf, " OWNER %s", fmtId(fsowner));
		if (fsfsys_is_local)
			appendPQExpBuffer(buf, " ON local");
		else
			appendPQExpBuffer(buf, " ON %s", fsfsys);
		appendPQExpBuffer(buf, " (\n");

		/*
		 * We still need to lookup all of the paths associated with this
		 * filespace, look them up in the pg_filespace_entry table.
		 */
		subbuf = createPQExpBuffer();
		appendPQExpBuffer(subbuf,
						  "SELECT fsedbid, regexp_replace(fselocation, '^[^/]*://({[^}]*}){0,1}', '') "
						  "FROM pg_catalog.pg_filespace_entry "
						  "WHERE fsefsoid = %s "
						  "ORDER BY 1",
						  fsoid);
		entries = executeQuery(conn, subbuf->data);
		destroyPQExpBuffer(subbuf);

		/* append the filespace location information to output */
		for (j = 0; j < PQntuples(entries); j++)
		{
			char	   *dbid = PQgetvalue(entries, j, 0);
			char	   *location = PQgetvalue(entries, j, 1);

			if (j > 0)
				appendPQExpBuffer(buf, ",\n");
			appendPQExpBuffer(buf, "  %s: ", dbid);
			appendStringLiteralConn(buf, location, conn);
		}
		PQclear(entries);

		appendPQExpBuffer(buf, "\n) WITH (NUMREPLICA = ");

		/* we need to look at the replica number of filespace*/
		subbuf = createPQExpBuffer();
		appendPQExpBuffer(subbuf,
						  "SELECT fsrep "
						  "FROM pg_filespace "
						  "WHERE oid = %s ",
						  fsoid);
		entries = executeQuery(conn, subbuf->data);
		destroyPQExpBuffer(subbuf);

		/* add replica number to output*/
		char	   *fsrep = PQgetvalue(entries, 0, 0);
		appendPQExpBuffer(buf, "%s);\n", fsrep);
		PQclear(entries);

		/* Add a comment on the filespace if specified */
		if (fsdesc && strlen(fsdesc))
		{
			appendPQExpBuffer(buf, "COMMENT ON FILESPACE %s IS ", fsname);
			appendStringLiteralConn(buf, fsdesc, conn);
			appendPQExpBuffer(buf, ";\n");
		}

		/* Output the results */
		fprintf(OPF, "%s", buf->data);

		free(fsname);
		destroyPQExpBuffer(buf);
	}

	PQclear(res);
	fprintf(OPF, "\n\n");
}

/*
 * Dump tablespaces.
 */
static void
dumpTablespaces(PGconn *conn)
{
	PGresult   *res;
	int			i;

	/*
	 * Get all tablespaces execpt built-in ones (named pg_xxx)
	 *
	 * [FIXME] the queries need to be slightly different if the backend isn't
	 * Greenplum, and the dump format should vary depending on if the dump is
	 * --gp-syntax or --no-gp-syntax.
	 */
	if (server_version < 80214)
	{							
		/* Filespaces were introduced in GP 4.0 (server_version 8.2.14) */
		return;
	}
	else
	{
		res = executeQuery(conn, "SELECT spcname, "
						 "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
						   "fsname, spcacl, "
					  "pg_catalog.shobj_description(t.oid, 'pg_tablespace') "
						   "FROM pg_catalog.pg_tablespace t, "
						   "pg_catalog.pg_filespace fs "
						   "WHERE t.spcfsoid = fs.oid AND spcname !~ '^pg_' "
						   "and spcname != 'dfs_default' ORDER BY 1");
	}

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Tablespaces\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		PQExpBuffer buf = createPQExpBuffer();
		char	   *spcname = PQgetvalue(res, i, 0);
		char	   *spcowner = PQgetvalue(res, i, 1);
		char	   *fsname = PQgetvalue(res, i, 2);
		char	   *spcacl = PQgetvalue(res, i, 3);
		char	   *spccomment = PQgetvalue(res, i, 4);

		/* needed for buildACLCommands() */
		spcname = strdup(fmtId(spcname));

		if (output_clean)
			appendPQExpBuffer(buf, "DROP TABLESPACE %s;\n", spcname);

		appendPQExpBuffer(buf, "CREATE TABLESPACE %s", spcname);
		appendPQExpBuffer(buf, " OWNER %s", fmtId(spcowner));
		appendPQExpBuffer(buf, " FILESPACE %s;\n", fmtId(fsname));

		/* Build Acls */
		if (!skip_acls &&
			!buildACLCommands(spcname, "TABLESPACE", spcacl, spcowner,
							  server_version, buf))
		{
			fprintf(stderr, _("%s: could not parse ACL list (%s) for tablespace \"%s\"\n"),
					progname, spcacl, spcname);
			PQfinish(conn);
			exit(1);
		}

		/* Set comments */
		if (spccomment && strlen(spccomment))
		{
			appendPQExpBuffer(buf, "COMMENT ON TABLESPACE %s IS ", spcname);
			appendStringLiteralConn(buf, spccomment, conn);
			appendPQExpBuffer(buf, ";\n");
		}

		fprintf(OPF, "%s", buf->data);

		free(spcname);
		destroyPQExpBuffer(buf);
	}

	PQclear(res);
	fprintf(OPF, "\n\n");
}

/*
 * Dump commands to create each database.
 *
 * To minimize the number of reconnections (and possibly ensuing
 * password prompts) required by the output script, we emit all CREATE
 * DATABASE commands during the initial phase of the script, and then
 * run pg_dump for each database to dump the contents of that
 * database.  We skip databases marked not datallowconn, since we'd be
 * unable to connect to them anyway (and besides, we don't want to
 * dump template0).
 */
static void
dumpCreateDB(PGconn *conn)
{
	PQExpBuffer buf = createPQExpBuffer();
	PGresult   *res;
	int			i;

	fprintf(OPF, "--\n-- Database creation\n--\n\n");

	res = executeQuery(conn,
					   "SELECT datname, "
					   "coalesce(rolname, (select rolname from pg_authid where oid=(select datdba from pg_database where datname='template0'))), "
					   "pg_encoding_to_char(d.encoding), "
					   "datistemplate, datacl, datconnlimit, "
					   "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace "
			  "FROM pg_database d LEFT JOIN pg_authid u ON (datdba = u.oid) "
					   "WHERE datallowconn ORDER BY 1");

	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *dbname = PQgetvalue(res, i, 0);
		char	   *dbowner = PQgetvalue(res, i, 1);
		char	   *dbencoding = PQgetvalue(res, i, 2);
		char	   *dbistemplate = PQgetvalue(res, i, 3);
		char	   *dbacl = PQgetvalue(res, i, 4);
		char	   *dbconnlimit = PQgetvalue(res, i, 5);
		char	   *dbtablespace = PQgetvalue(res, i, 6);
		char	   *fdbname;

		fdbname = strdup(fmtId(dbname));

		resetPQExpBuffer(buf);

		/*
		 * Skip the CREATE DATABASE commands for "template0" and "postgres",
		 * since they are presumably already there in the destination cluster.
		 * We do want to emit their ACLs and config options if any, however.
		 */
		if (strcmp(dbname, "template0") != 0 &&
			strcmp(dbname, "postgres") != 0 &&
			strcmp(dbname, "template1") != 0)
		{
			if (output_clean)
				appendPQExpBuffer(buf, "DROP DATABASE %s;\n", fdbname);

			appendPQExpBuffer(buf, "CREATE DATABASE %s", fdbname);

			appendPQExpBuffer(buf, " WITH TEMPLATE = template1");

			if (strlen(dbowner) != 0)
				appendPQExpBuffer(buf, " OWNER = %s", fmtId(dbowner));

			appendPQExpBuffer(buf, " ENCODING = ");
			appendStringLiteralConn(buf, dbencoding, conn);

			/*
			 * Output tablespace if it isn't the default.  For default, it
			 * uses the default from the template database.  If tablespace is
			 * specified and tablespace creation failed earlier, (e.g. no such
			 * directory), the database creation will fail too.  One solution
			 * would be to use 'SET default_tablespace' like we do in pg_dump
			 * for setting non-default database locations.
			 */
			if (strcmp(dbtablespace, "pg_default") != 0)
				appendPQExpBuffer(buf, " TABLESPACE = %s",
								  fmtId(dbtablespace));

			if (strcmp(dbconnlimit, "-1") != 0)
				appendPQExpBuffer(buf, " CONNECTION LIMIT = %s",
								  dbconnlimit);

			appendPQExpBuffer(buf, ";\n");

			if (strcmp(dbistemplate, "t") == 0)
			{
				appendPQExpBuffer(buf, "UPDATE pg_database SET datistemplate = 't' WHERE datname = ");
				appendStringLiteralConn(buf, dbname, conn);
				appendPQExpBuffer(buf, ";\n");
			}
		}

		if (!skip_acls &&
			!buildACLCommands(fdbname, "DATABASE", dbacl, dbowner,
							  server_version, buf))
		{
			fprintf(stderr, _("%s: could not parse ACL list (%s) for database \"%s\"\n"),
					progname, dbacl, fdbname);
			PQfinish(conn);
			exit(1);
		}

		fprintf(OPF, "%s", buf->data);

		dumpDatabaseConfig(conn, dbname);

		free(fdbname);
	}

	PQclear(res);
	destroyPQExpBuffer(buf);

	fprintf(OPF, "\n\n");
}



/*
 * Dump database-specific configuration
 */
static void
dumpDatabaseConfig(PGconn *conn, const char *dbname)
{
	PQExpBuffer buf = createPQExpBuffer();
	int			count = 1;

	for (;;)
	{
		PGresult   *res;

		printfPQExpBuffer(buf, "SELECT datconfig[%d] FROM pg_database WHERE datname = ", count);
		appendStringLiteralConn(buf, dbname, conn);
		appendPQExpBuffer(buf, ";");

		res = executeQuery(conn, buf->data);
		if (!PQgetisnull(res, 0, 0))
		{
			makeAlterConfigCommand(conn, PQgetvalue(res, 0, 0),
								   "DATABASE", dbname);
			PQclear(res);
			count++;
		}
		else
		{
			PQclear(res);
			break;
		}
	}

	destroyPQExpBuffer(buf);
}



/*
 * Dump user-specific configuration
 */
static void
dumpUserConfig(PGconn *conn, const char *username)
{
	PQExpBuffer buf = createPQExpBuffer();
	int			count = 1;

	for (;;)
	{
		PGresult   *res;

		printfPQExpBuffer(buf, "SELECT rolconfig[%d] FROM pg_authid WHERE rolname = ", count);

		appendStringLiteralConn(buf, username, conn);

		res = executeQuery(conn, buf->data);
		if (PQntuples(res) == 1 &&
			!PQgetisnull(res, 0, 0))
		{
			makeAlterConfigCommand(conn, PQgetvalue(res, 0, 0),
								   "ROLE", username);
			PQclear(res);
			count++;
		}
		else
		{
			PQclear(res);
			break;
		}
	}

	destroyPQExpBuffer(buf);
}



/*
 * Helper function for dumpXXXConfig().
 */
static void
makeAlterConfigCommand(PGconn *conn, const char *arrayitem,
					   const char *type, const char *name)
{
	char	   *pos;
	char	   *mine;
	PQExpBuffer buf = createPQExpBuffer();

	mine = strdup(arrayitem);
	pos = strchr(mine, '=');
	if (pos == NULL)
		return;

	*pos = 0;
	appendPQExpBuffer(buf, "ALTER %s %s ", type, fmtId(name));
	appendPQExpBuffer(buf, "SET %s TO ", fmtId(mine));

	/*
	 * Some GUC variable names are 'LIST' type and hence must not be quoted.
	 */
	if (pg_strcasecmp(mine, "DateStyle") == 0
		|| pg_strcasecmp(mine, "search_path") == 0)
		appendPQExpBuffer(buf, "%s", pos + 1);
	else
		appendStringLiteralConn(buf, pos + 1, conn);
	appendPQExpBuffer(buf, ";\n");

	fprintf(OPF, "%s", buf->data);
	destroyPQExpBuffer(buf);
	free(mine);
}



/*
 * Dump contents of databases.
 */
static void
dumpDatabases(PGconn *conn)
{
	PGresult   *res;
	int			i;

	res = executeQuery(conn, "SELECT datname FROM pg_database WHERE datallowconn ORDER BY 1");

	for (i = 0; i < PQntuples(res); i++)
	{
		int			ret;

		char	   *dbname = PQgetvalue(res, i, 0);

		if (verbose)
			fprintf(stderr, _("%s: dumping database \"%s\"...\n"), progname, dbname);

		fprintf(OPF, "\\connect %s\n\n", fmtId(dbname));

		if (filename)
			fclose(OPF);

		ret = runPgDump(dbname);
		if (ret != 0)
		{
			fprintf(stderr, _("%s: pg_dump failed on database \"%s\", exiting\n"), progname, dbname);
			exit(1);
		}

		if (filename)
		{
			OPF = fopen(filename, PG_BINARY_A);
			if (!OPF)
			{
				fprintf(stderr, _("%s: could not re-open the output file \"%s\": %s\n"),
						progname, filename, strerror(errno));
				exit(1);
			}
		}

	}

	PQclear(res);
}



/*
 * Run pg_dump on dbname.
 */
static int
runPgDump(const char *dbname)
{
	PQExpBuffer cmd = createPQExpBuffer();
	const char *p;
	int			ret;

	/*
	 * Win32 has to use double-quotes for args, rather than single quotes.
	 * Strangely enough, this is the only place we pass a database name on the
	 * command line, except "postgres" which doesn't need quoting.
	 */
#ifndef WIN32
	appendPQExpBuffer(cmd, "%s\"%s\" %s -Fp '", SYSTEMQUOTE, pg_dump_bin,
#else
	appendPQExpBuffer(cmd, "%s\"%s\" %s -Fp \"", SYSTEMQUOTE, pg_dump_bin,
#endif
					  pgdumpopts->data);

	/* Shell quoting is not quite like SQL quoting, so can't use fmtId */
	for (p = dbname; *p; p++)
	{
#ifndef WIN32
		if (*p == '\'')
			appendPQExpBuffer(cmd, "'\"'\"'");
#else
		if (*p == '"')
			appendPQExpBuffer(cmd, "\\\"");
#endif
		else
			appendPQExpBufferChar(cmd, *p);
	}

#ifndef WIN32
	appendPQExpBufferChar(cmd, '\'');
#else
	appendPQExpBufferChar(cmd, '"');
#endif

	appendPQExpBuffer(cmd, "%s", SYSTEMQUOTE);

	if (verbose)
		fprintf(stderr, _("%s: running \"%s\"\n"), progname, cmd->data);

	fflush(stdout);
	fflush(stderr);

	ret = system(cmd->data);

	destroyPQExpBuffer(cmd);

	return ret;
}



/*
 * Make a database connection with the given parameters.  An
 * interactive password prompt is automatically issued if required.
 *
 * If fail_on_error is false, we return NULL without printing any message
 * on failure, but preserve any prompted password for the next try.
 */
static PGconn *
connectDatabase(const char *dbname, const char *pghost, const char *pgport,
	   const char *pguser, enum trivalue prompt_password, bool fail_on_error)
{
	PGconn	   *conn;
	bool		new_pass;
	const char *remoteversion_str;
	int			my_version;
	static char *password = NULL;

	if (prompt_password == TRI_YES && !password)
		password = simple_prompt("Password: ", 100, false);

	/*
	 * Start the connection.  Loop until we have a password if requested by
	 * backend.
	 */
	do
	{
#define PARAMS_ARRAY_SIZE	8
		const char **keywords = malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
		const char **values = malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

		if (!keywords || !values)
		{
			fprintf(stderr, _("%s: out of memory\n"), progname);
			exit(1);
		}

		keywords[0] = "host";
		values[0] = pghost;
		keywords[1] = "port";
		values[1] = pgport;
		keywords[2] = "user";
		values[2] = pguser;
		keywords[3] = "password";
		values[3] = password;
		keywords[4] = "dbname";
		values[4] = dbname;
		keywords[5] = "fallback_application_name";
		values[5] = progname;
		keywords[6] = "options";
		values[6] = "-c gp_session_role=utility";
		keywords[7] = NULL;
		values[7] = NULL;

		new_pass = false;
		conn = PQconnectdbParams(keywords, values, true);

		free(keywords);
		free(values);

		if (!conn)
		{
			fprintf(stderr, _("%s: could not connect to database \"%s\"\n"),
					progname, dbname);
			exit(1);
		}

		if (PQstatus(conn) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(conn) &&
			password == NULL &&
			prompt_password != TRI_NO)
		{
			PQfinish(conn);
			password = simple_prompt("Password: ", 100, false);
			new_pass = true;
		}
	} while (new_pass);

	/* check to see that the backend connection was successfully made */
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		if (fail_on_error)
		{
			fprintf(stderr,
					_("%s: could not connect to database \"%s\": %s\n"),
					progname, dbname, PQerrorMessage(conn));
			exit(1);
		}
		else
		{
			PQfinish(conn);
			return NULL;
		}
	}

	remoteversion_str = PQparameterStatus(conn, "server_version");
	if (!remoteversion_str)
	{
		fprintf(stderr, _("%s: could not get server version\n"), progname);
		exit(1);
	}
	server_version = parse_version(remoteversion_str);
	if (server_version < 0)
	{
		fprintf(stderr, _("%s: could not parse server version \"%s\"\n"),
				progname, remoteversion_str);
		exit(1);
	}

	my_version = parse_version(PG_VERSION);
	if (my_version < 0)
	{
		fprintf(stderr, _("%s: could not parse version \"%s\"\n"),
				progname, PG_VERSION);
		exit(1);
	}

	if (my_version != server_version
		&& (server_version < 80200		/* we can handle back to 8.2 */
			|| server_version > my_version))
	{
		fprintf(stderr, _("server version: %s; %s version: %s\n"),
				remoteversion_str, progname, PG_VERSION);
		if (ignoreVersion)
			fprintf(stderr, _("proceeding despite version mismatch\n"));
		else
		{
			fprintf(stderr, _("aborting because of version mismatch  (Use the -i option to proceed anyway.)\n"));
			exit(1);
		}
	}

	/*
	 * On 7.3 and later, make sure we are not fooled by non-system schemas in
	 * the search path.
	 */
	executeCommand(conn, "SET search_path = pg_catalog");

	return conn;
}


/*
 * Run a query, return the results, exit program on failure.
 */
static PGresult *
executeQuery(PGconn *conn, const char *query)
{
	PGresult   *res;

	if (verbose)
		fprintf(stderr, _("%s: executing %s\n"), progname, query);

	res = PQexec(conn, query);
	if (!res ||
		PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, _("%s: query failed: %s"),
				progname, PQerrorMessage(conn));
		fprintf(stderr, _("%s: query was: %s\n"),
				progname, query);
		PQfinish(conn);
		exit(1);
	}

	return res;
}

/*
 * As above for a SQL command (which returns nothing).
 */
static void
executeCommand(PGconn *conn, const char *query)
{
	PGresult   *res;

	if (verbose)
		fprintf(stderr, _("%s: executing %s\n"), progname, query);

	res = PQexec(conn, query);
	if (!res ||
		PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, _("%s: query failed: %s"),
				progname, PQerrorMessage(conn));
		fprintf(stderr, _("%s: query was: %s\n"),
				progname, query);
		PQfinish(conn);
		exit(1);
	}

	PQclear(res);
}


/*
 * dumpTimestamp
 */
static void
dumpTimestamp(char *msg)
{
	char		buf[256];
	time_t		now = time(NULL);

	/*
	 * We don't print the timezone on Win32, because the names are long and
	 * localized, which means they may contain characters in various random
	 * encodings; this has been seen to cause encoding errors when reading the
	 * dump script.
	 */
	if (strftime(buf, sizeof(buf),
#ifndef WIN32
				 "%Y-%m-%d %H:%M:%S %Z",
#else
				 "%Y-%m-%d %H:%M:%S",
#endif
				 localtime(&now)) != 0)
		fprintf(OPF, "-- %s %s\n\n", msg, buf);
}
