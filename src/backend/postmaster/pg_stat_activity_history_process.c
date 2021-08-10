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
/* ----------
 * pg_stat_activity_history_process.c
 * ----------
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "pgstat.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "catalog/catquery.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "executor/instrument.h"
#include "pg_trace.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/backendid.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "utils/portal.h"
#include "cdb/cdbvars.h"

#include "pg_stat_activity_history_process.h"

#include "utils/timestamp.h"
#include "postgres_ext.h"
#include "libpq/pqcomm.h"

#include "pgtime.h"
#include "postmaster/syslogger.h"

/* ----------
 * Timer definitions.
 * ----------
 */
#define PGSTATACTIVITYHISTORY_RESTART_INTERVAL 60		/* How often to attempt to restart a
										 	 	 	 	 	 	 	 * failed pg_stat_activity_history process; in
										 	 	 	 	 	 	 	 * seconds. */

#define PGSTATACTIVITYHISTORY_SELECT_TIMEOUT	2		/* How often to check for postmaster
										 * death; in seconds. */

#define MAXQUERYLENGTH 5120			/* The maximum length of query we can write into
										 * pg_stat_activity_history table */

int pgStatActivityHistorySock = -1;

/* ----------
 * Local data
 * ----------
 */

static struct sockaddr_storage pgStatActivityHistoryAddr;

static time_t last_pgStatActivityHistory_start_time;

static volatile bool need_exit = false;

static volatile bool got_SIGHUP = false;

/* ----------
 * Local function forward declarations
 * ----------
 */
#ifdef EXEC_BACKEND
static pid_t pgstatactivityhistory_forkexec(void);
#endif

NON_EXEC_STATIC void PgStatActivityHistoryMain(int argc, char *argv[]);
static void pgstatactivityhistory_exit(SIGNAL_ARGS);
static void pgstatactivityhistory_sighup_handler(SIGNAL_ARGS);
static void WriteDataIntoPSAH(FILE *fp, void *data);

/* ------------------------------------------------------------
 * Public functions called from postmaster follow
 * ------------------------------------------------------------
 */

/* ----------
 * pgstatactivityhistory_init() -
 *
 *	Called from postmaster at startup. Create the resources required
 *	by the pg stat activity history process.  If unable to do so, do not
 *	fail --- better to let the postmaster start with stats collection
 *	disabled.
 * ----------
 */
void
pgstatactivityhistory_init(void)
{
	socklen_t	alen;
	struct addrinfo *addrs = NULL,
			   *addr,
				hints;
	int			ret;
	fd_set		rset;
	struct timeval tv;
	char		test_byte;
	int			sel_res;
	int			tries = 0;

#define TESTBYTEVAL ((char) 199)

	/*
	 * Create the UDP socket for sending and receiving statistic messages
	 */
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = PF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_protocol = 0;
	hints.ai_addrlen = 0;
	hints.ai_addr = NULL;
	hints.ai_canonname = NULL;
	hints.ai_next = NULL;
	ret = pg_getaddrinfo_all("localhost", NULL, &hints, &addrs);
	if (ret || !addrs)
	{
		ereport(LOG,
				(errmsg("could not resolve \"localhost\": %s",
						gai_strerror(ret))));
		goto startup_failed;
	}

	/*
	 * On some platforms, pg_getaddrinfo_all() may return multiple addresses
	 * only one of which will actually work (eg, both IPv6 and IPv4 addresses
	 * when kernel will reject IPv6).  Worse, the failure may occur at the
	 * bind() or perhaps even connect() stage.	So we must loop through the
	 * results till we find a working combination. We will generate LOG
	 * messages, but no error, for bogus combinations.
	 */
	for (addr = addrs; addr; addr = addr->ai_next)
	{
#ifdef HAVE_UNIX_SOCKETS
		/* Ignore AF_UNIX sockets, if any are returned. */
		if (addr->ai_family == AF_UNIX)
			continue;
#endif

		if (++tries > 1)
			ereport(LOG,
			(errmsg("trying another address for history logger process")));

		/*
		 * Create the socket.
		 */
		if ((pgStatActivityHistorySock = socket(addr->ai_family, SOCK_DGRAM, 0)) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
			errmsg("could not create socket for history logger process: %m")));
			continue;
		}

		/*
		 * Bind it to a kernel assigned port on localhost and get the assigned
		 * port via getsockname().
		 */
		if (bind(pgStatActivityHistorySock, addr->ai_addr, addr->ai_addrlen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
			  errmsg("could not bind socket for history logger process: %m")));
			closesocket(pgStatActivityHistorySock);
			pgStatActivityHistorySock = -1;
			continue;
		}

		alen = sizeof(pgStatActivityHistoryAddr);
		if (getsockname(pgStatActivityHistorySock, (struct sockaddr *) & pgStatActivityHistoryAddr, &alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not get address of socket for history logger process: %m")));
			closesocket(pgStatActivityHistorySock);
			pgStatActivityHistorySock = -1;
			continue;
		}

		/*
		 * Connect the socket to its own address.  This saves a few cycles by
		 * not having to respecify the target address on every send. This also
		 * provides a kernel-level check that only packets from this same
		 * address will be received.
		 */
		if (connect(pgStatActivityHistorySock, (struct sockaddr *) & pgStatActivityHistoryAddr, alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
			errmsg("could not connect socket for history logger process: %m")));
			closesocket(pgStatActivityHistorySock);
			pgStatActivityHistorySock = -1;
			continue;
		}

		/*
		 * Try to send and receive a one-byte test message on the socket. This
		 * is to catch situations where the socket can be created but will not
		 * actually pass data (for instance, because kernel packet filtering
		 * rules prevent it).
		 */
		test_byte = TESTBYTEVAL;

retry1:
		if (send(pgStatActivityHistorySock, &test_byte, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry1;	/* if interrupted, just retry */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not send test message on socket for history logger process: %m")));
			closesocket(pgStatActivityHistorySock);
			pgStatActivityHistorySock = -1;
			continue;
		}

		/*
		 * There could possibly be a little delay before the message can be
		 * received.  We arbitrarily allow up to half a second before deciding
		 * it's broken.
		 */
		for (;;)				/* need a loop to handle EINTR */
		{
			FD_ZERO(&rset);
			FD_SET		(pgStatActivityHistorySock, &rset);

			tv.tv_sec = 0;
			tv.tv_usec = 500000;
			sel_res = select(pgStatActivityHistorySock + 1, &rset, NULL, NULL, &tv);
			if (sel_res >= 0 || errno != EINTR)
				break;
		}
		if (sel_res < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("select() failed in history logger process: %m")));
			closesocket(pgStatActivityHistorySock);
			pgStatActivityHistorySock = -1;
			continue;
		}
		if (sel_res == 0 || !FD_ISSET(pgStatActivityHistorySock, &rset))
		{
			/*
			 * This is the case we actually think is likely, so take pains to
			 * give a specific message for it.
			 *
			 * errno will not be set meaningfully here, so don't use it.
			 */
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("test message did not get through on socket for history logger process")));
			closesocket(pgStatActivityHistorySock);
			pgStatActivityHistorySock = -1;
			continue;
		}

		test_byte++;			/* just make sure variable is changed */

retry2:
		if (recv(pgStatActivityHistorySock, &test_byte, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry2;	/* if interrupted, just retry */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not receive test message on socket for history logger process: %m")));
			closesocket(pgStatActivityHistorySock);
			pgStatActivityHistorySock = -1;
			continue;
		}

		if (test_byte != TESTBYTEVAL)	/* strictly paranoia ... */
		{
			ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("incorrect test message transmission on socket for history logger process")));
			closesocket(pgStatActivityHistorySock);
			pgStatActivityHistorySock = -1;
			continue;
		}

		/* If we get here, we have a working socket */
		break;
	}

	/* Did we find a working address? */
	if (!addr || pgStatActivityHistorySock < 0)
		goto startup_failed;

	/*
	 * Set the socket to non-blocking IO.  This ensures that if the collector
	 * falls behind, statistics messages will be discarded; backends won't
	 * block waiting to send messages to the collector.
	 */
	if (!pg_set_noblock(pgStatActivityHistorySock))
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not set history logger process socket to nonblocking mode: %m")));
		goto startup_failed;
	}

	pg_freeaddrinfo_all(hints.ai_family, addrs);

	return;

startup_failed:
	ereport(LOG,
	  (errmsg("disabling history logger process for lack of working socket")));

	if (addrs)
		pg_freeaddrinfo_all(hints.ai_family, addrs);

	if (pgStatActivityHistorySock >= 0)
		closesocket(pgStatActivityHistorySock);
	pgStatActivityHistorySock = -1;
}

#ifdef EXEC_BACKEND

/*
 * pgstatactivityhistory_forkexec() -
 *
 * Format up the arglist for, then fork and exec, pg_stat_activity_history process
 */
static pid_t
pgstatactivityhistory_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkcol";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */

	av[ac] = NULL;
	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif   /* EXEC_BACKEND */

/*
 * pgstatactivityhistory_start() -
 *
 *	Called from postmaster at startup or after an existing collector
 *	died.  Attempt to write query information in pg_stat_activity_history
 *
 *	Returns PID of child process, or 0 if fail.
 *
 *	Note: if fail, we will be called again from the postmaster main loop.
 */

int
pgstatactivityhistory_start(void)
{
	time_t		curtime;
	pid_t		pgStatActivityHistroyPID;

	/*
	 * Check that the socket is there, else pgstatactivityhistory_init failed and we can do
	 * nothing useful.
	 */
	if (pgStatActivityHistorySock < 0)
		return 0;

	/*
	 * Do nothing if too soon since last collector start.  This is a safety
	 * valve to protect against continuous respawn attempts if the collector
	 * is dying immediately at launch.	Note that since we will be re-called
	 * from the postmaster main loop, we will get another chance later.
	 */
	curtime = time(NULL);
	if ((unsigned int) (curtime - last_pgStatActivityHistory_start_time) <
		(unsigned int) PGSTATACTIVITYHISTORY_RESTART_INTERVAL)
		return 0;
	last_pgStatActivityHistory_start_time = curtime;

	/*
	 * Okay, fork off pg_stat_activity_history process.
	 */
#ifdef EXEC_BACKEND
	switch ((pgStatActivityHistroyPID = pgstatactivityhistory_forkexec()))
#else
	switch ((pgStatActivityHistroyPID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork history logger process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			/* Lose the postmaster's on-exit routines */
			on_exit_reset();

			/* Drop our connection to postmaster's shared memory, as well */
			PGSharedMemoryDetach();
			PgStatActivityHistoryMain(0, NULL);
			break;
#endif

		default:
			return (int) pgStatActivityHistroyPID;
	}

	/* shouldn't get here */
	return 0;
}

void
allow_immediate_pgStatActivityHistory_restart(void)
{
	last_pgStatActivityHistory_start_time = 0;
}


/* ----------
 * PgstatCollectorMain() -
 *
 *	Start up the statistics collector process.	This is the body of the
 *	postmaster child process.
 *
 *	The argc/argv parameters are valid only in EXEC_BACKEND case.
 * ----------
 */
NON_EXEC_STATIC void
PgStatActivityHistoryMain(int argc, char *argv[])
{
	int 			len;
	void			*msg = palloc(sizeof(queryHistoryInfo) + MAXQUERYLENGTH);

#ifndef WIN32
#ifdef HAVE_POLL
	struct pollfd input_fd;
#else
	struct timeval sel_timeout;
	fd_set		rfds;
#endif
#endif

	IsUnderPostmaster = true;	/* we are a postmaster subprocess now */

	MyProcPid = getpid();		/* reset MyProcPid */

	MyStartTime = time(NULL);	/* record Start Time for logging */

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(pgstat probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

	/*
	 * Ignore all signals usually bound to some action in the postmaster,
	 * except SIGQUIT.
	 */
	pqsignal(SIGHUP, pgstatactivityhistory_sighup_handler);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SIG_IGN);
	pqsignal(SIGQUIT, pgstatactivityhistory_exit);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);
	PG_SETMASK(&UnBlockSig);

	/*
	 * Identify myself via ps
	 */
	init_ps_display("history logger process", "", "", "");

	/*
	 * Setup the descriptor set for select(2).	Since only one bit in the set
	 * ever changes, we need not repeat FD_ZERO each time.
	 */
#if !defined(HAVE_POLL) && !defined(WIN32)
	FD_ZERO(&rfds);
#endif

	/*
	 * open pg_stat_activity_history txt file
	 */
	struct pg_tm *tm;
	pg_time_t timestamp = time(NULL);
	tm = pg_localtime(&timestamp, log_timezone);
	FILE *fp = NULL;
	char filestr[1024];
	memset(filestr, '\0', 1024);
	int res = snprintf(filestr, 1024, "%s/%s/hawq-%d-%.2d-%.2d-%.2d%.2d%.2d.history",
	                   DataDir, Log_directory, (tm->tm_year+1900), tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec);
	if (res < 0)
		elog(WARNING, "hawq history file's path length is bigger than 1024! The array need expanded.\n");
	if((fp = fopen(filestr, "a")) == NULL)
		elog(ERROR, "%s cannot be opened!\n", filestr);
	/*
	 * Loop to process messages until we get SIGQUIT or detect ungraceful
	 * death of our parent postmaster.
	 *
	 * For performance reasons, we don't want to do a PostmasterIsAlive() test
	 * after every message; instead, do it only when select()/poll() is
	 * interrupted by timeout.	In essence, we'll stay alive as long as
	 * backends keep sending us stuff often, even if the postmaster is gone.
	 */
	for (;;)
	{
		int			got_data;

		/*
		 * Quit if we get SIGQUIT from the postmaster.
		 */
		if (need_exit)
			break;

		/*
		 * Reload configuration if we got SIGHUP from the postmaster.
		 */
		if (got_SIGHUP)
		{
			ProcessConfigFile(PGC_SIGHUP);
			got_SIGHUP = false;
		}

		/*
		 * Wait for a message to arrive; but not for more than
		 * PGSTAT_SELECT_TIMEOUT seconds. (This determines how quickly we will
		 * shut down after an ungraceful postmaster termination; so it needn't
		 * be very fast.  However, on some systems SIGQUIT won't interrupt the
		 * poll/select call, so this also limits speed of response to SIGQUIT,
		 * which is more important.)
		 *
		 * We use poll(2) if available, otherwise select(2). Win32 has its own
		 * implementation.
		 */
#ifndef WIN32
#ifdef HAVE_POLL
		input_fd.fd = pgStatActivityHistorySock;
		input_fd.events = POLLIN | POLLERR;
		input_fd.revents = 0;

		if (poll(&input_fd, 1, PGSTATACTIVITYHISTORY_SELECT_TIMEOUT * 1000) < 0)
		{
			if (errno == EINTR)
				continue;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("poll() failed in history logger process: %m")));
		}

		got_data = (input_fd.revents != 0);
#else							/* !HAVE_POLL */

		FD_SET(pgStatActivityHistorySock, &rfds);

		/*
		 * timeout struct is modified by select() on some operating systems,
		 * so re-fill it each time.
		 */
		sel_timeout.tv_sec = PGSTATACTIVITYHISTORY_SELECT_TIMEOUT;
		sel_timeout.tv_usec = 0;

		if (select(pgStatActivityHistorySock + 1, &rfds, NULL, NULL, &sel_timeout) < 0)
		{
			if (errno == EINTR)
				continue;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("select() failed in history logger process: %m")));
		}

		got_data = FD_ISSET(pgStatActivityHistorySock, &rfds);
#endif   /* HAVE_POLL */
#else /* WIN32 */
		got_data = pgwin32_waitforsinglesocket(pgStatActivityHistorySock, FD_READ,
		                                       PGSTATACTIVITYHISTORY_SELECT_TIMEOUT*1000);
#endif

		/*
		 * If there is a message on the socket, read it and check for
		 * validity.
		 */
		if (got_data)
		{
			len = recv(pgStatActivityHistorySock, (char *) msg,
					   sizeof(queryHistoryInfo) + MAXQUERYLENGTH, 0);
			if (len < 0)
			{
				if (errno == EINTR)
					continue;
				ereport(ERROR,
						(errcode_for_socket_access(),
						 errmsg("could not read statistics message: %m")));
			}
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				WriteDataIntoPSAH(fp, (void *)msg);
				fflush(fp);
			}
		}
		else
		{
			/*
			 * We can only get here if the select/poll timeout elapsed. Check
			 * for postmaster death.
			 */
			if (!PostmasterIsAlive(true))
				break;
		}
	}							/* end of message-processing loop */
	fclose(fp);
	pfree(msg);
	exit(0);
}

/* SIGQUIT signal handler for pg_stat_activity_history process */
static void
pgstatactivityhistory_exit(SIGNAL_ARGS)
{
	need_exit = true;
}

/* SIGHUP handler for pg_stat_activity_history process */
static void
pgstatactivityhistory_sighup_handler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

/*
 * checkQuery()
 *
 * We don't write BEGIN/ABORT/COMMIT query into history table
 */
static bool isQueryValid(const char *query)
{
	const char *str1 = "BEGIN";
	const char *str2 = "ABORT";
	const char *str3 = "COMMIT";

	if (pg_strncasecmp(query, str1, 5) == 0 || pg_strncasecmp(query, str2, 5) == 0 || pg_strncasecmp(query, str3, 6) == 0)
		return false;
	return true;
}

/*
 * pgStatActivityHistory_send()
 *
 * Send query information to pg_stat_activity_history process
 */
void pgStatActivityHistory_send(Oid databaseId, Oid userId, int processId,
                 Oid sessionId, const char *creation_time, const char *end_time,
                 struct Port *tmpProcPort, char *application_name, double cpuUsage,
                 uint32_t memoryUsage, int status, char *errorInfo,const char *query)
{

	// check if current query is valid
	if (!isQueryValid(query))
		return ;

	void *data;
	int copy_len = 0, i, j, send_size = 0;
	const char *longQuery = "Query is too long!";

	queryHistoryInfo *qhi;

	if (pgStatActivityHistorySock == PGINVALID_SOCKET)
		return;
	if (MAXQUERYLENGTH - 1 < strlen(query))
	{
		send_size = sizeof(queryHistoryInfo) + strlen(longQuery);
		data = palloc(send_size);
		qhi = (queryHistoryInfo*)data;
		qhi->queryLen = strlen(longQuery);
		memcpy((char *)data + sizeof(queryHistoryInfo), longQuery, strlen(longQuery));
	}
	else
	{
		send_size = sizeof(queryHistoryInfo) + strlen(query);
		data = palloc(send_size);
		qhi = (queryHistoryInfo*)data;
		qhi->queryLen = strlen(query);
		memcpy((char *)data + sizeof(queryHistoryInfo), query, strlen(query));
	}

	qhi->databaseId = databaseId;

	char* database_name = caql_getcstring(
		                  NULL,
		                  cql("SELECT datname FROM pg_database "
		                      " WHERE oid = :1 ",
		                      ObjectIdGetDatum(databaseId)));
	memset(qhi->database_name, '\0', MAXDATABASENAME);
	if (MAXDATABASENAME - 1 < strlen(database_name))
	{
		elog(WARNING, "The database name array size is too small!"
				"(database name size : %lu)\n", strlen(database_name));
		copy_len = MAXDATABASENAME - 1;
	}
	else
		copy_len = strlen(database_name);
	strncpy(qhi->database_name, database_name, copy_len);

	qhi->userId = userId;
	int fetchCount;
	char* user_name = caql_getcstring_plus(
							NULL,
							&fetchCount,
							NULL,
							cql("SELECT rolname FROM pg_authid  WHERE oid = :1 ",
								ObjectIdGetDatum(userId)));
	memset(qhi->user_name, '\0', MAXUSERNAME);
	if (MAXUSERNAME - 1 < strlen(user_name))
	{
		elog(WARNING, "The user name array size is too small!"
				"(user name size : %lu)\n", strlen(user_name));
		copy_len = MAXUSERNAME - 1;
	}
	else
		copy_len = strlen(user_name);
	strncpy(qhi->user_name, user_name, copy_len);

	qhi->processId = processId;
	qhi->sessionId = sessionId;

	memset(qhi->creation_time, '\0', MAXTIMELENGTH);
	if (MAXTIMELENGTH - 1 < strlen(creation_time))
	{
		elog(WARNING, "The creation_time array size is too small!"
				"(creation_time size : %lu)\n", strlen(creation_time));
		copy_len = MAXTIMELENGTH - 1;
	}
	else
		copy_len = strlen(creation_time);
	strncpy(qhi->creation_time, creation_time, copy_len);

	memset(qhi->end_time, '\0', MAXTIMELENGTH);
	if (MAXTIMELENGTH - 1 < strlen(end_time))
	{
		elog(WARNING, "The end_time array size is too small!"
				"(end_time size : %lu)\n", strlen(end_time));
		copy_len = MAXTIMELENGTH - 1;
	}
	else
		copy_len = strlen(end_time);
	strncpy(qhi->end_time, end_time, copy_len);

	memset(qhi->client_addr, '\0', MAXCLIENTADDRLENGTH);
	SockAddr clientaddr;
	if (tmpProcPort)
	{
		memcpy(&clientaddr, &tmpProcPort->raddr, sizeof(clientaddr));
	}
	else
	{
		MemSet(&clientaddr, 0, sizeof(clientaddr));
	}
	SockAddr  zero_clientaddr;
	memset(&zero_clientaddr, 0, sizeof(zero_clientaddr));
	if (memcmp(&(clientaddr), &zero_clientaddr,
	           (size_t) (sizeof(zero_clientaddr) == 0)))
	{
		qhi->client_port = -2;
	}
	else
	{
		if (clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
				|| clientaddr.addr.ss_family == AF_INET6
#endif
			)
		{
			char    remote_host[NI_MAXHOST];
			char    remote_port[NI_MAXSERV];
			int     ret;

			remote_host[0] = '\0';
			remote_port[0] = '\0';
			ret = pg_getnameinfo_all(&clientaddr.addr,
			clientaddr.salen,
			remote_host, sizeof(remote_host),
			remote_port, sizeof(remote_port),
			NI_NUMERICHOST | NI_NUMERICSERV);
			if (ret)
			{
				qhi->client_port = -2;
			}
			else
			{
				clean_ipv6_addr(clientaddr.addr.ss_family, remote_host);
				if (MAXCLIENTADDRLENGTH - 1 < strlen(remote_host))
				{
					elog(WARNING, "The application name array size is too small!"
							"(application name size : %lu)\n", strlen(remote_host));
					copy_len = MAXCLIENTADDRLENGTH - 1;
				}
				else
					copy_len = strlen(remote_host);
				strncpy(qhi->client_addr, remote_host, copy_len);
				qhi->client_port = atoi(remote_port);
			}
		}
		else if (clientaddr.addr.ss_family == AF_UNIX)
		{
			qhi->client_port = -1;
		}
		else
		{
			qhi->client_port = -2;
		}
	}

	memset(qhi->application_name, '\0', MAXAPPNAMELENGTH);
	if (MAXAPPNAMELENGTH - 1 < strlen(application_name))
	{
		elog(WARNING, "The application name array size is too small!"
				"(application name size : %lu)\n", strlen(application_name));
		copy_len = MAXAPPNAMELENGTH - 1;
	}
	else
		copy_len = strlen(application_name);
	strncpy(qhi->application_name, application_name, copy_len);

	qhi->cpuUsage = cpuUsage;
	qhi->memoryUsage = memoryUsage;

	memset(qhi->status, '\0', MAXSTATUSLENGTH);
	if (status == 1)
		strncpy(qhi->status, "success", 7);
	else if (status == -1)
		strncpy(qhi->status, "failed", 6);
	else
		strncpy(qhi->status, "waiting", 7);

	memset(qhi->errorInfo, '\0', MAXERRORINFOLENGTH);
	for (i = 0, j = 0; i < MAXERRORINFOLENGTH - 1 && j < strlen(errorInfo); ++i, ++j)
	{
		if (errorInfo[j] == '"')
			qhi->errorInfo[i++] = '"';
		qhi->errorInfo[i] = errorInfo[j];
	}
	if (j < strlen(errorInfo))
		elog(WARNING, "The error information array size is too small!"
				"(error information size : %lu)\n", strlen(errorInfo));

	int rc;
	/* We'll retry after EINTR, but ignore all other failures */
	do
	{
		rc = send(pgStatActivityHistorySock, (void *)data, send_size, 0);
	} while (rc < 0 && errno == EINTR);
	pfree(data);

	is_qtype_sql = false;

#ifdef USE_ASSERT_CHECKING
	/* In debug builds, log send failures ... */
	if (rc < 0)
		elog(LOG, "could not send to history logger process: %m");
#endif

}

static void
WriteDataIntoPSAH(FILE *fp, void *data)
{
	int i, j;
	queryHistoryInfo *qhi = (queryHistoryInfo *)data;
	char *recv_query = (char *)data + sizeof(queryHistoryInfo);
	char *final_query = palloc(2*(qhi->queryLen));
	memset(final_query, '\0', 2*(qhi->queryLen));

	for (i = 0, j = 0; j < qhi->queryLen; ++i, ++j)
	{
		if (recv_query[j] == '"')
			final_query[i++] = '"';
		final_query[i] = recv_query[j];
	}

	if (qhi->client_port != -2)
		fprintf(fp,"%d,\"%s\",%d,\"%s\",%d,%d,\"%s\",\"%s\",\"%s\",%d,\"%s\",\"%.3f vcore\",\"%d MB\",\"%s\",\"%s\",\"%s\"\n",
						qhi->databaseId, qhi->database_name, qhi->userId, qhi->user_name, qhi->processId, qhi->sessionId,
						qhi->creation_time, qhi->end_time, qhi->client_addr, qhi->client_port, qhi->application_name,
						qhi->cpuUsage, qhi->memoryUsage, qhi->status, qhi->errorInfo, final_query);
	else
		fprintf(fp,"%d,\"%s\",%d,\"%s\",%d,%d,\"%s\",\"%s\",\"%s\",,\"%s\",\"%.3f vcore\",\"%d MB\",\"%s\",\"%s\",\"%s\"\n",
						qhi->databaseId, qhi->database_name, qhi->userId, qhi->user_name, qhi->processId, qhi->sessionId,
						qhi->creation_time, qhi->end_time, qhi->client_addr, qhi->application_name,
						qhi->cpuUsage, qhi->memoryUsage, qhi->status, qhi->errorInfo, final_query);

	pfree(final_query);
}
