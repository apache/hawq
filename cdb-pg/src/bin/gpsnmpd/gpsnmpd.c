/*-------------------------------------------------------------------------
 * gpsnmpd.c
 *
 *      Global definitions for the main PostgreSQL SNMP Daemon.
 *
 *      Copyright (c) 2004-2005, PostgreSQL Global Development Group
 *      Author: Hiroshi Saito
 *
 *      $Id: gpsnmpd.c,v 1.29 2009/08/17 17:41:57 eggyknap Exp $
 *-------------------------------------------------------------------------
 */

/* TODO: Make this respond to all snmpd command opts when called as a standalone daemon */

#include "gpsnmpd.h"
#include "rdbmsDbTable/rdbmsDbTable.h"
#include "rdbmsDbInfoTable/rdbmsDbInfoTable.h"
#include "rdbmsDbParamTable/rdbmsDbParamTable.h"
#include "rdbmsDbLimitedResourceTable/rdbmsDbLimitedResourceTable.h"
#include "rdbmsSrvTable/rdbmsSrvTable.h"
#include "rdbmsSrvParamTable/rdbmsSrvParamTable.h"
#include "rdbmsSrvInfoTable/rdbmsSrvInfoTable.h"
#include "rdbmsRelTable/rdbmsRelTable.h"

/*#include "PGSQL-MIB_src/pgsqlCatalogTables/pgsqlPgAggregateTable/pgsqlPgAggregateTable.h"
 #include "PGSQL-MIB_src/pgsqlCatalogTables/pgsqlPgAmopTable/pgsqlPgAmopTable.h" */

#include "customquery.h"

#include <signal.h>
#include <ctype.h>

/*
 * define strings
 */

#define GPSNMP_DAEMON "GPDB-SNMP-Ver2.0 for Greenplum Database 4.x"

#define SUB_AGENT    1

char *pgsnmp_version = PG_VERSION_STR;

/* Official MIB object 
 http://www.iana.org/assignments/enterprise-numbers

 31327
 Greenplum
 Ben Werther
 bwerther&greenplum.com
 */

/*
 * PostgreSQL Connection
 */
PGconn *dbconn = NULL;
char *conninfo = { "dbname=postgres" };
#define     SEARCH_PATH "set search_path TO public;"

static int keep_running = 0;
struct timeval snmp_boottime;
static int pgsqlState = 0;

static PGconn *
db_connect(char *constring)
{
	PGconn *dbconn;
	dbconn = PQconnectdb(constring);
	if (dbconn == NULL)
	{
		return NULL;
	}
	if (PQstatus(dbconn) != CONNECTION_OK)
	{
		snmp_log(LOG_ERR, "%s\n", PQerrorMessage(dbconn));
		PQfinish(dbconn);
		return NULL;
	}

	return dbconn;
}

bool IsAlive()
{
	PGresult *qryRes;

	if (!pgsqlState)
	{
		if ((dbconn = db_connect(conninfo)))
			pgsqlState = 1;
		else
			return false;
	}

	/* TODO: Deal with setting the schema properly here */
	qryRes = PQexec(dbconn, "SELECT 1;");
	if (PQresultStatus(qryRes) != PGRES_TUPLES_OK)
	{
		PQclear(qryRes);
		PQfinish(dbconn);
		pgsqlState = 0;
		dbconn = NULL;
		return false;
	}

	PQclear(qryRes);
	return true;
}

static void stop_server(int signo)
{
	pgsqlState = keep_running = 0;
	snmp_log(LOG_ERR, "\nExiting on signal %d\n", signo);
}

static void init_mibs(void)
{
	/* This initializes the table MIB.  */

	initialize_table_rdbmsDbTable();
	init_rdbmsDbInfoTable();
	init_rdbmsDbParamTable();
	init_rdbmsDbLimitedResourceTable();
	init_rdbmsSrvTable();
	init_rdbmsSrvParamTable();
	init_rdbmsSrvInfoTable();
	init_rdbmsRelTable();
#if 0
	init_custom_queries();
#endif
}

static void usage(void)
{
	/*
	 * The doc people want me to display the usage from a file they write, which is odd
	 * So, first I'll try to find their file, and if found, I'll display it.
	 */
	char* GPHOME = 0;
	FILE* fp = 0;

	GPHOME = getenv("GPHOME");

	if (GPHOME && GPHOME[0] != '\0')
	{
		char path_to_help[1024];
		snprintf(path_to_help,1024,"%s/docs/cli_help/gpsnmpd_help",GPHOME);
		fp = fopen(path_to_help, "r");
	}

	if (fp)
	{
		int i;
		while ((i = getc(fp)) != EOF)
				putchar(i);
		fclose(fp);
		fflush(stdout);
	}
	else
	{

		fprintf(stderr, "Version %s\n\n", GPSNMP_DAEMON);
		fprintf(stderr,
				"usage: gpsnmpd [-s] [-b] [-c FILE ] [-x address ] [-g] [-C \"Connect String\"]\n\n");
		fprintf(stderr, "   -s : run as AgentX sub-agent of an existing snmpd process.  Recommended.  You do no need to use -x with this option.");
		fprintf(stderr, "   -b : run in the background\n");
		fprintf(stderr, "   -c : configuration file name (often not needed if running as a subagent)\n");
		fprintf(stderr, "   -g : use syslog to log errors in gpsnmpd\n");
		fprintf(stderr, "   -C : libpq connect strings\n");
		fprintf(stderr, "   -Q : custom query configuration file\n");
		fprintf(stderr, "   -x : address:port of a network interface\n");
		/*	fprintf(stderr, "   -n : database search_path to use for gpsnmpd-specific tables (default: $user, public)\n"); */
		fprintf(stderr, "   -V : display version strings\n");
		fprintf(stderr, "   -m MIB[:...]\t\tload given list of MIBs (ALL loads everything)\n");
		fprintf(stderr, "   -M DIR[:...]\t\tlook in given list of directories for MIBs\n");
	}
}

static void version(void)
{
	fprintf(stderr, "Version %s\n", GPSNMP_DAEMON);
	fflush(stderr);
}



int main(int argc, char *argv[])
{
	int agentx_subagent = 0; /* SNMP master agent */
	int background = 0; /* run the background */
	int syslog = 0; /* use syslog */

	int arg;
	/*	char *set_search_path, *escaped_search_path;
	 int errorval;*/

#ifdef DEBUG
	char *strv;
#endif

	char options[128] = "sbhc:gm:M:HC:x:V-:Q:";
	/*char	 options[128] = "sbc:gC:x:V-:n:"; */
	PGresult *qryRes;

	while ((arg = getopt(argc, argv, options)) != EOF)
	{
		switch (arg)
		{
		case '-':
			if (strcasecmp(optarg, "help") == 0)
			{
				usage();
				exit(0);
			}
			if (strcasecmp(optarg, "version") == 0)
			{
				version();
				exit(0);
			}

			handle_long_opt(optarg);
			break;


		case 'V':
			version();
			exit(0);
		case '?':
		case 'h':
			usage();
			exit(0);

		case 'Q':
			if (optarg != NULL)
				custom_query_config_file = optarg;
			else
			{
				usage();
				exit(-1);
			}
		case 'c':
			if (optarg != NULL)
			{
				netsnmp_ds_set_string(NETSNMP_DS_LIBRARY_ID,
						NETSNMP_DS_LIB_OPTIONALCONFIG, optarg);
			}
			else
			{
				usage();
				exit(-1);
			}
			break;
		case 'D':
			debug_register_tokens(optarg);
			snmp_set_do_debugging(1);
			break;

		case 'g':
			syslog = 1;
			break;
        case 'H':
            init_agent("gpsnmpd");
            init_snmp("gpsnmpd");
            fprintf(stderr, "Configuration directives understood:\n");
            read_config_print_usage("  ");
            exit(0);

		case 'b':
			background = 1;
			break;
		case 's':
			agentx_subagent = 1;
			break;
		case 'm':
			if (optarg != NULL)
			{
				setenv("MIBS", optarg, 1);
			}
			else
			{
				usage();
				exit(1);
			}
		    break;

		case 'M':
			if (optarg != NULL)
			{
				setenv("MIBDIRS", optarg, 1);
			}
			else
			{
				usage();
				exit(1);
			}
			break;


		case 'x':
			if (optarg != NULL)
			{
				printf("-x option: %s\n", optarg);
				netsnmp_ds_set_string(NETSNMP_DS_APPLICATION_ID,
						NETSNMP_DS_AGENT_X_SOCKET, optarg);
			}
			else
			{
				usage();
				exit(-1);
			}
			break;
			/*		case 'n':
			 if (optarg != NULL) search_path = optarg;
			 else {
			 usage();
			 exit(1);
			 } */
		case 'C':
			if (optarg != NULL)
			{
				conninfo = strdup(optarg);
				/* Remove the string from the args because there might be a password */
				memset(optarg, '\0', strlen(optarg));
			}
			else
			{
				usage();
				exit(1);
			}
			break;
			/*            default:
			 usage();
			 exit(1); */
		}
	}

	gettimeofday(&snmp_boottime, (struct timezone *) 0);

	if (syslog)
		/* Some problems are contained.
		 snmp_enable_calllog(); */
		snmp_enable_syslog();
	else
		snmp_enable_stderrlog();


	/* Add RDBMS-MIB GPDB-MIB to the list to be loaded */
	const char * env = getenv("MIBS");
	if (env==NULL || env[0] == '\0')
		putenv("MIBS=+RDBMS-MIB:NETWORK-SERVICES-MIB:GPDB-MIB:SNMP-FRAMEWORK-MIB:SNMPv2-CONF:SNMPv2-TC:SNMPv2-TC");


	if (getuid() != 0)
		netsnmp_ds_set_boolean(NETSNMP_DS_APPLICATION_ID,
				NETSNMP_DS_AGENT_NO_ROOT_ACCESS, 1);

	do
	{
		dbconn = db_connect(conninfo);
		if (!dbconn)
		{
			snmp_log(LOG_INFO,
					"Waiting to retry DB connect -- sleeping 10 seconds\n");
			sleep(10);
		}
	} while (!dbconn);

	pgsqlState = 1;

	qryRes = PQexec(dbconn, SEARCH_PATH);

	if (PQresultStatus(qryRes) != PGRES_COMMAND_OK)
		snmp_log(LOG_ERR,
				"Could not set search path because of some problem with the set query\n");

	PQclear(qryRes);

#if 0
	if (strncmp(search_path, "$user, public", 13) != 0)
	{
		escaped_search_path = calloc(sizeof(*search_path) * 2 + 1, sizeof(char*));
		if (escaped_search_path == NULL)
		{
			snmp_log(LOG_ERR, "Could not allocate memory to escape search_path value\n");
			exit(1);
		}
		PQescapeStringConn(dbconn, escaped_search_path, search_path, strlen(search_path), &errorval); */
		/* This initialization is to avoid a compiler warning */
		set_search_path = NULL;
		if (errorval != 0)
		snmp_log(LOG_ERR, "Could not escape search path string\n");
		/* Note: I'm not exiting after this error. The only problem with not setting the
		 * search_path is not having the database tables available, which right now isn't
		 * a really big deal */
		else
		{
			set_search_path = calloc(sizeof(*escaped_search_path) + sizeof("SET search_path TO "), sizeof(char*));
			if (set_search_path == NULL)
			snmp_log(LOG_ERR, "Could not allocate memory to set search_path\n");
			else
			{
				snmp_log(LOG_INFO, set_search_path);
				sprintf(set_search_path, "SET search_path TO %s", escaped_search_path);
				qryRes = PQexec(dbconn, set_search_path);
				if (PQresultStatus(qryRes) != PGRES_COMMAND_OK) snmp_log(LOG_ERR, "Could not set search path because of some problem with the set query\n");
				PQclear(qryRes);
			}
		}
		free(escaped_search_path); free(set_search_path);
	}
#endif

	/*   }
	 else {
	 printf("Failure connecting to PostgreSQL. gpsnmpd will exit\n");
	 exit(1);
	 }*/

	/*
	 * Log that we are up and monitoring the database.  Ignore any error from this command
	 */
	qryRes = PQexec(dbconn, "select gp_elog('gpsnmpd is running and monitoring the system')");
	PQclear(qryRes);


	if (agentx_subagent)
	{
		/* make us a agentx client. */
		netsnmp_ds_set_boolean(NETSNMP_DS_APPLICATION_ID,
				NETSNMP_DS_AGENT_ROLE, SUB_AGENT);

#if 0
		netsnmp_ds_set_boolean(NETSNMP_DS_APPLICATION_ID,
				NETSNMP_DS_AGENT_AGENTX_MASTER, 0);

#endif
#ifdef DEBUG
		if (netsnmp_ds_get_boolean(NETSNMP_DS_APPLICATION_ID,
						NETSNMP_DS_AGENT_ROLE))
		snmp_log(LOG_INFO,"SUB_AGENT running.\n");
#endif

	}
#if 0
	netsnmp_ds_set_int(NETSNMP_DS_APPLICATION_ID,
			NETSNMP_DS_AGENT_AGENTX_TIMEOUT, -1);
	netsnmp_ds_set_int(NETSNMP_DS_APPLICATION_ID,
			NETSNMP_DS_AGENT_AGENTX_RETRIES, -1);

	netsnmp_ds_register_config(ASN_OCTET_STR, "snmp", "public",
			NETSNMP_DS_LIBRARY_ID, NETSNMP_DS_LIB_COMMUNITY);

#endif
#ifdef DEBUG
	if(netsnmp_ds_get_string(NETSNMP_DS_APPLICATION_ID,
					NETSNMP_DS_AGENT_X_SOCKET))
	snmp_log(LOG_INFO,"AGENT_X_SOCKET:%s\n",
			netsnmp_ds_get_string(NETSNMP_DS_APPLICATION_ID,
					NETSNMP_DS_AGENT_X_SOCKET));

	if(netsnmp_ds_get_string(NETSNMP_DS_APPLICATION_ID,
					NETSNMP_DS_AGENT_PORTS))
	snmp_log(LOG_INFO,"AGENT_PORTS:%s\n",
			netsnmp_ds_get_string(NETSNMP_DS_APPLICATION_ID,
					NETSNMP_DS_AGENT_PORTS));
#endif

	/* run in background */
	if (background && netsnmp_daemonize(1, !syslog))
		exit(1);

	/* initialize tcpip */
	SOCK_STARTUP;

	init_agent(GPSNMP_DAEMON);

	init_mibs();

	init_snmp(GPSNMP_DAEMON);

	if (!agentx_subagent)
		init_master_agent();

	keep_running = 1;

	signal(SIGTERM, stop_server);
	signal(SIGINT, stop_server);

	snmp_log(LOG_INFO, "%s is up and running.\n", GPSNMP_DAEMON);

	while (keep_running)
	{
		agent_check_and_process(1);
#ifdef DEBUG
		strv = netsnmp_ds_get_string(NETSNMP_DS_LIBRARY_ID,
				NETSNMP_DS_LIB_COMMUNITY );
		if (strv)
		snmp_log(LOG_INFO, strv);
#endif

	}

	/*
	 * Log that we are shutting down the agent.  Ignore any error from this command
	 */
	if (pgsqlState == 1)
	{
		qryRes = PQexec(dbconn, "select gp_elog('gpsnmpd has stopped')");
		PQclear(qryRes);
	}

	/* shutdown */
	if (!agentx_subagent)
		snmp_shutdown(GPSNMP_DAEMON);

	SOCK_CLEANUP;

	return 0;

}
