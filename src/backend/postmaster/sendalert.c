/*-------------------------------------------------------------------------
 *
 * alert.c
 *
 * Send alerts via SMTP (email) or SNMP INFORM messsages.
 *
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
 *
 *-------------------------------------------------------------------------
 */
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE<600
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif
#if !defined(_POSIX_C_SOURCE) || _POSIX_C_SOURCE<200112L
#undef _POSIX_C_SOURCE
/* Define to activate features from IEEE Stds 1003.1-2001 */
#define _POSIX_C_SOURCE 200112L
#endif
#include "postgres.h"
#include "pg_config.h"  /* Adding this helps eclipse see that USE_EMAIL and USE_SNMP are set */

#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <stdio.h>

#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>

#include "lib/stringinfo.h"

#include "pgtime.h"

#include "postmaster/syslogger.h"
#include "postmaster/sendalert.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "sendalert_common.h"

extern int	PostPortNumber;

#ifdef USE_EMAIL
#ifdef USE_SSL
#include <openssl/ssl.h>
#endif
/* SASL (RFC 2222) client library API */
#include <auth-client.h>
#include <libesmtp.h>
#endif

#if USE_SNMP
#include <net-snmp/net-snmp-config.h>
#include <net-snmp/definitions.h>
#include <net-snmp/types.h>

//#include <net-snmp/utilities.h>
#include <net-snmp/session_api.h>
#include <net-snmp/pdu_api.h>
#include <net-snmp/mib_api.h>
#include <net-snmp/varbind_api.h>
//#include <net-snmp/config_api.h>
#include <net-snmp/output_api.h>


oid             objid_enterprise[] = { 1, 3, 6, 1, 4, 1, 3, 1, 1 };
oid             objid_sysdescr[] = { 1, 3, 6, 1, 2, 1, 1, 1, 0 };
oid             objid_sysuptime[] = { 1, 3, 6, 1, 2, 1, 1, 3, 0 };
oid             objid_snmptrap[] = { 1, 3, 6, 1, 6, 3, 1, 1, 4, 1, 0 };
oid				objid_rdbmsrelstate[] = { 1, 3, 6, 1, 2, 1, 39, 1, 9, 1, 1 };

// {iso(1) identified-organization(3) dod(6) internet(1) private(4) enterprises(1) gpdbMIB(31327) gpdbObjects(1) gpdbAlertMsg(1)}
oid				objid_gpdbAlertMsg[] = { 1, 3, 6, 1, 4, 1, 31327, 1, 1 };
oid				objid_gpdbAlertSeverity[] = { 1, 3, 6, 1, 4, 1, 31327, 1, 2 };
oid				objid_gpdbAlertSqlstate[] = { 1, 3, 6, 1, 4, 1, 31327, 1, 3 };
oid				objid_gpdbAlertDetail[] = { 1, 3, 6, 1, 4, 1, 31327, 1, 4 };
oid				objid_gpdbAlertSqlStmt[] = { 1, 3, 6, 1, 4, 1, 31327, 1, 5 };
oid				objid_gpdbAlertSystemName[] = { 1, 3, 6, 1, 4, 1, 31327, 1, 6 };
#endif

static bool SplitString(char *rawstring, char delimiter, List **namelist);

#ifdef USE_EMAIL

static char * get_str_from_chunk(CSVChunkStr *chunkstr,
		const PipeProtoChunk *saved_chunks);
static const char * messagebody_cb(void **buf, int *len, void *arg);
static void print_recipient_status(smtp_recipient_t recipient,
		const char *mailbox, void *arg  __attribute__((unused)));
static int authinteract(auth_client_request_t request, char **result,
		int fields, void *arg);
static int tlsinteract(char *buf, int buflen, int rwflag  __attribute__((unused)), void *arg  __attribute__((unused)));
static int handle_invalid_peer_certificate(long vfy_result);
static void event_cb(smtp_session_t session, int event_no, void *arg, ...);
static void monitor_cb (const char *buf, int buflen, int writing, void *arg);

static int send_alert_via_email(const GpErrorData * errorData, const char * subject, const char * email_priority);
#endif

#if USE_SNMP
static int send_snmp_inform_or_trap();
extern pg_time_t	MyStartTime;
#endif


#ifdef USE_EMAIL

int send_alert_from_chunks(const PipeProtoChunk *chunk,
		const PipeProtoChunk * saved_chunks_in)
{

	int ret = -1;
	GpErrorData errorData;

	CSVChunkStr chunkstr =
	{ chunk, chunk->data + sizeof(GpErrorDataFixFields) };

	memset(&errorData, 0, sizeof(errorData));

	memcpy(&errorData.fix_fields, chunk->data, sizeof(errorData.fix_fields));

	if (chunk == NULL)
		return -1;
	if (chunk->hdr.len == 0)
		return -1;
	if (chunk->hdr.zero != 0)
		return -1;

	if (chunk->hdr.log_format != 'c')
		elog(ERROR,"send_alert_from_chunks only works when CSV logging is enabled");

	errorData.username = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.databasename = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.remote_host = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.remote_port = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.error_severity = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.sql_state = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.error_message = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.error_detail = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.error_hint = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.internal_query = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.error_context = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.debug_query_string = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.error_func_name = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.error_filename = get_str_from_chunk(&chunkstr,saved_chunks_in);
	errorData.stacktrace = get_str_from_chunk(&chunkstr,saved_chunks_in);


	PG_TRY();
	{
	ret = send_alert(&errorData);
	}
	PG_CATCH();
	{
		elog(LOG,"send_alert failed.  Not sending the alert");
		free(errorData.stacktrace ); errorData.stacktrace = NULL;
		free((char *)errorData.error_filename ); errorData.error_filename = NULL;
		free((char *)errorData.error_func_name ); errorData.error_func_name = NULL;
		free(errorData.debug_query_string ); errorData.debug_query_string = NULL;
		free(errorData.error_context); errorData.error_context = NULL;
		free(errorData.internal_query ); errorData.internal_query = NULL;
		free(errorData.error_hint ); errorData.error_hint = NULL;
		free(errorData.error_detail ); errorData.error_detail = NULL;
		free(errorData.error_message ); errorData.error_message = NULL;
		free(errorData.sql_state ); errorData.sql_state = NULL;
		free((char *)errorData.error_severity ); errorData.error_severity = NULL;
		free(errorData.remote_port ); errorData.remote_port = NULL;
		free(errorData.remote_host ); errorData.remote_host = NULL;
		free(errorData.databasename ); errorData.databasename = NULL;
		free(errorData.username ); errorData.username = NULL;
		/* Carry on with error handling. */
		PG_RE_THROW();
	}
	PG_END_TRY();

	// Don't forget to free them!  Best in reverse order of the mallocs.

	free(errorData.stacktrace ); errorData.stacktrace = NULL;
	free((char *)errorData.error_filename ); errorData.error_filename = NULL;
	free((char *)errorData.error_func_name ); errorData.error_func_name = NULL;
	free(errorData.debug_query_string ); errorData.debug_query_string = NULL;
	free(errorData.error_context); errorData.error_context = NULL;
	free(errorData.internal_query ); errorData.internal_query = NULL;
	free(errorData.error_hint ); errorData.error_hint = NULL;
	free(errorData.error_detail ); errorData.error_detail = NULL;
	free(errorData.error_message ); errorData.error_message = NULL;
	free(errorData.sql_state ); errorData.sql_state = NULL;
	free((char *)errorData.error_severity ); errorData.error_severity = NULL;
	free(errorData.remote_port ); errorData.remote_port = NULL;
	free(errorData.remote_host ); errorData.remote_host = NULL;
	free(errorData.databasename ); errorData.databasename = NULL;
	free(errorData.username ); errorData.username = NULL;

	return ret;
}

#endif

#if USE_SNMP
static int send_snmp_inform_or_trap(const GpErrorData * errorData, const char * subject, const char * severity)
{

	netsnmp_session session, *ss = NULL;
	netsnmp_pdu    *pdu, *response;
	int				status;
	char            csysuptime[20];
	static bool 	snmp_initialized = false;
	static char myhostname[255];	/* gethostname usually is limited to 65 chars out, but make this big to be safe */
	char	   *rawstring = NULL;
	List	   *elemlist = NIL;
	ListCell   *l = NULL;

	/*
	 * "inform" messages get a positive acknowledgement response from the SNMP manager.
	 * If it doesn't come, the message might be resent.
	 *
	 * "trap" messages are one-way, and we have no idea if the manager received it.
	 * But, it's faster and cheaper, and no need to retry.  So some people might prefer it.
	 */
	bool inform = strcmp(gp_snmp_use_inform_or_trap,"inform") == 0;


	if (gp_snmp_monitor_address == NULL || gp_snmp_monitor_address[0] == '\0')
	{
		static bool firsttime = 1;

		ereport(firsttime ? LOG : DEBUG1,(errmsg("SNMP inform/trap alerts are disabled"),errOmitLocation(true)));
		firsttime = false;

		return -1;
	}


	/*
	 * SNMP managers are required to handle messages up to at least 484 bytes long, but I believe most existing
	 * managers support messages up to one packet (ethernet frame) in size, 1472 bytes.
	 *
	 * But, should we take that chance?  Or play it safe and limit the message to 484 bytes?
	 */

	elog(DEBUG2,"send_snmp_inform_or_trap");

	if (!snmp_initialized)
	{
		snmp_enable_stderrlog();

		if (gp_snmp_debug_log != NULL && gp_snmp_debug_log[0] != '\0')
		{
			snmp_enable_filelog(gp_snmp_debug_log, 1);

			//debug_register_tokens("ALL");
			snmp_set_do_debugging(1);
		}

		/*
		 * Initialize the SNMP library.  This also reads the MIB database.
		 */
		/* Add GPDB-MIB to the list to be loaded */
		putenv("MIBS=+GPDB-MIB:SNMP-FRAMEWORK-MIB:SNMPv2-CONF:SNMPv2-TC:SNMPv2-TC");

		init_snmp("sendalert");

		snmp_initialized = true;

		{
			char portnum[16];
			myhostname[0] = '\0';

			if (gethostname(myhostname, sizeof(myhostname)) == 0)
			{
				strcat(myhostname,":");
				pg_ltoa(PostPortNumber,portnum);
				strcat(myhostname,portnum);
			}
		}
	}

	/*
	 * Trap/Inform messages always start with the system up time. (SysUpTime.0)
	 *
	 * This presumably would be the uptime of GPDB, not the machine it is running on, I think.
	 *
	 * Use Postmaster's "MyStartTime" as a way to get that.
	 */

	sprintf(csysuptime, "%ld", (long)(time(NULL) - MyStartTime));


	/*
	// ERRCODE_DISK_FULL could be reported vi rbmsMIB rdbmsTraps rdbmsOutOfSpace trap.
	// But it appears we never generate that error?

	// ERRCODE_ADMIN_SHUTDOWN means SysAdmin aborted somebody's request.  Not interesting?

	// ERRCODE_CRASH_SHUTDOWN sounds interesting, but I don't see that we ever generate it.

	// ERRCODE_CANNOT_CONNECT_NOW means we are starting up, shutting down, in recovery, or Too many users are logged on.

	// abnormal database system shutdown
	*/



	/*
	 * The gpdbAlertSeverity is a crude attempt to classify some of these messages based on severity,
	 * where OK means everything is running normal, Down means everything is shut down, degraded would be
	 * for times when some segments are down, but the system is up, The others are maybe useful in the future
	 *
	 *  gpdbSevUnknown(0),
	 *	gpdbSevOk(1),
	 *	gpdbSevWarning(2),
	 *	gpdbSevError(3),
	 *	gpdbSevFatal(4),
	 *	gpdbSevPanic(5),
	 *	gpdbSevSystemDegraded(6),
	 *	gpdbSevSystemDown(7)
	 */


	char detail[MAX_ALERT_STRING+1];
	snprintf(detail, MAX_ALERT_STRING, "%s", errorData->error_detail);
	detail[127] = '\0';

	char sqlstmt[MAX_ALERT_STRING+1];
	char * sqlstmtp = errorData->debug_query_string;
	if (sqlstmtp == NULL || sqlstmtp[0] == '\0')
		sqlstmtp = errorData->internal_query;
	if (sqlstmtp == NULL)
		sqlstmtp = "";


	snprintf(sqlstmt, MAX_ALERT_STRING, "%s", sqlstmtp);
	sqlstmt[MAX_ALERT_STRING] = '\0';


	/* Need a modifiable copy of To list */
	rawstring = pstrdup(gp_snmp_monitor_address);

	/* Parse string into list of identifiers */
	if (!SplitString(rawstring, ';', &elemlist))
	{
		/* syntax error in list */
		ereport(LOG,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax for \"gp_snmp_monitor_address\"")));
		return -1;
	}


	/*
	 * This session is just a template, and doesn't need to be connected.
	 * It is used by snmp_add(), which copies this info, opens the new session, and assigns the transport.
	 */
	snmp_sess_init( &session );	/* Initialize session to default values */
	session.version = SNMP_VERSION_2c;
	session.timeout = SNMP_DEFAULT_TIMEOUT;
	session.retries = SNMP_DEFAULT_RETRIES;
	session.remote_port = 162; 							// I think this isn't used by net-snmp any more.

	/*if (strchr(session.peername,':')==NULL)
		strcat(session.peername,":162");*/
	session.community = (u_char *)gp_snmp_community;
	session.community_len = strlen((const char *)session.community);  // SNMP_DEFAULT_COMMUNITY_LEN means "public"
	session.callback_magic = NULL;

	foreach(l, elemlist)
	{
		char	   *cur_gp_snmp_monitor_address = (char *) lfirst(l);

		if (cur_gp_snmp_monitor_address == NULL || cur_gp_snmp_monitor_address[0] == '\0')
			continue;

		session.peername = cur_gp_snmp_monitor_address;
		/*
		 * If we try to "snmp_open( &session )", net-snmp will set up a connection to that
		 * endpoint on port 161, assuming we are the network monitor, and the other side is an agent.
		 *
		 * But we are pretending to be an agent, sending traps to the NM, so we don't need this.
		 */

		/*if (!snmp_open( &session ))   	// Don't open the session here!
		{
			const char     *str;
			int             xerr;
			xerr = snmp_errno;
			str = snmp_api_errstring(xerr);
			elog(LOG, "snmp_open: %s", str);
			return -1;
		}*/

		/*
		 * This call copies the info from "session" to "ss", assigns the transport, and opens the session.
		 * We must specify "snmptrap" so the transport will know we want port 162 by default.
		 */
		ss = snmp_add(&session,
					  netsnmp_transport_open_client("snmptrap", cur_gp_snmp_monitor_address),
					  NULL, NULL);
		if (ss == NULL) {
			/*
			 * diagnose netsnmp_transport_open_client and snmp_add errors with
			 * the input netsnmp_session pointer
			 */
			{
				char           *err;
				snmp_error(&session, NULL, NULL, &err);
				elog(LOG, "send_alert snmp_add of %s failed: %s", cur_gp_snmp_monitor_address, err);
				free(err);
			}
			return -1;
		}

		/*
		 * We need to create the pdu each time, as it gets freed when we send a trap.
		 */
		pdu = snmp_pdu_create(inform ? SNMP_MSG_INFORM : SNMP_MSG_TRAP2);
		if (!pdu)
		{
			const char     *str;
			int             xerr;
			xerr = snmp_errno;
			str = snmp_api_errstring(xerr);
			elog(LOG, "Failed to create notification PDU: %s", str);
			return -1;
		}

		/*
		 * Trap/Inform messages always start with the system up time. (SysUpTime.0)
		 * We use Postmaster's "MyStartTime" as a way to get that.
		 */

		snmp_add_var(pdu, objid_sysuptime,
							sizeof(objid_sysuptime) / sizeof(oid),
							't', (const char *)csysuptime);

	#if 0
		/*
		 * In the future, we might want to send RDBMS-MIB::rdbmsStateChange when the system becomes unavailable or
		 * partially unavailable.  This code, which is not currently used, shows how to build the pdu for
		 * that trap.
		 */
		/* {iso(1) identified-organization(3) dod(6) internet(1) mgmt(2) mib-2(1) rdbmsMIB(39) rdbmsTraps(2) rdbmsStateChange(1)} */
		snmp_add_var(pdu, objid_snmptrap,
							sizeof(objid_snmptrap) / sizeof(oid),
							'o', "1.3.6.1.2.1.39.2.1");  // rdbmsStateChange


		snmp_add_var(pdu, objid_rdbmsrelstate,
							sizeof(objid_rdbmsrelstate) / sizeof(oid),
							'i', "5");  // 4 = restricted, 5 = unavailable
	#endif

		/* {iso(1) identified-organization(3) dod(6) internet(1) private(4) enterprises(1) gpdbMIB(31327) gpdbTraps(5) gpdbTrapsList(0) gpdbAlert(1)} */

		/*
		 * We could specify this trap oid by name, rather than numeric oid, but then if the GPDB-MIB wasn't
		 * found, we'd get an error.  Using the numeric oid means we can still work without the MIB loaded.
		 */
		snmp_add_var(pdu, objid_snmptrap,
							sizeof(objid_snmptrap) / sizeof(oid),
							'o', "1.3.6.1.4.1.31327.5.0.1");  // gpdbAlert


		snmp_add_var(pdu, objid_gpdbAlertMsg,
							sizeof(objid_gpdbAlertMsg) / sizeof(oid),
							's', subject);  // SnmpAdminString = UTF-8 text
		snmp_add_var(pdu, objid_gpdbAlertSeverity,
							sizeof(objid_gpdbAlertSeverity) / sizeof(oid),
							'i', (char *)severity);

		snmp_add_var(pdu, objid_gpdbAlertSqlstate,
							sizeof(objid_gpdbAlertSqlstate) / sizeof(oid),
							's', errorData->sql_state);

		snmp_add_var(pdu, objid_gpdbAlertDetail,
							sizeof(objid_gpdbAlertDetail) / sizeof(oid),
							's', detail); // SnmpAdminString = UTF-8 text

		snmp_add_var(pdu, objid_gpdbAlertSqlStmt,
							sizeof(objid_gpdbAlertSqlStmt) / sizeof(oid),
							's', sqlstmt); // SnmpAdminString = UTF-8 text

		snmp_add_var(pdu, objid_gpdbAlertSystemName,
							sizeof(objid_gpdbAlertSystemName) / sizeof(oid),
							's', myhostname); // SnmpAdminString = UTF-8 text



		elog(DEBUG2,"ready to send to %s",cur_gp_snmp_monitor_address);
		if (inform)
			status = snmp_synch_response(ss, pdu, &response);
		else
			status = snmp_send(ss, pdu) == 0;

		elog(DEBUG2,"send, status %d",status);
		if (status != STAT_SUCCESS)
		{
			/* Something went wrong */
			if (ss)
			{
				char           *err;
				snmp_error(ss, NULL, NULL, &err);
				elog(LOG, "sendalert failed to send %s: %s", inform ? "inform" : "trap", err);
				free(err);
			}
			else
			{
				elog(LOG, "sendalert failed to send %s: %s", inform ? "inform" : "trap", "Something went wrong");
			}
			if (!inform)
				snmp_free_pdu(pdu);
		}
		else if (inform)
			snmp_free_pdu(response);

		snmp_close(ss);
		ss = NULL;

	}

	/*
	 * don't do the shutdown, to avoid the cost of starting up snmp each time
	 * (plus, it doesn't seem to work to run snmp_init() again after a shutdown)
	 *
	 * It would be nice to call this when the syslogger is shutting down.
	 */
	/*snmp_shutdown("sendalert");*/


	return 0;
}
#endif

#ifdef USE_EMAIL
static int send_alert_via_email(const GpErrorData * errorData,
		const char * subject, const char * email_priority)
{
	smtp_session_t session;
	smtp_message_t message;
	smtp_recipient_t recipient = NULL;
	auth_context_t authctx;
	const smtp_status_t *status;

	enum notify_flags notify = Notify_FAILURE | Notify_DELAY;
	char *rawstring = NULL;
	List *elemlist = NIL;
	ListCell *l = NULL;
	int success = 0;
        static int num_connect_failures = 0;
        static time_t last_connect_failure_ts = 0;


        if (gp_email_connect_failures && num_connect_failures >= gp_email_connect_failures) {
            if (time(0) - last_connect_failure_ts > gp_email_connect_avoid_duration) {
                num_connect_failures = 0;
                elog(LOG, "Retrying emails now...");
            } else {
                elog(LOG, "Not attempting emails as of now");
                return -1;
            }
        }

	if (gp_email_to == NULL || strlen(gp_email_to) == 0)
	{
		static bool firsttime = 1;

		ereport(firsttime ? LOG : DEBUG1,(errmsg("e-mail alerts are disabled"),errOmitLocation(true)));
		firsttime = false;
		return -1;
	}

	if (strlen(gp_email_from) == 0)
	{
		elog(LOG,"e-mail alerts are not properly configured:  No 'from:' address configured");
		return -1;
	}

	if (strchr(gp_email_to, ';') == NULL && strchr(gp_email_to, ',') != NULL)
	{
		// email addrs should be separated by ';', but because we used to require ',',
		// let's accept that if it looks OK.
		while (strchr(gp_email_to,',') != NULL)
		*strchr(gp_email_to,',') = ';';
	}

	// Ok, send the alert here.

	static bool auth_client_init_called = false;
	if (!auth_client_init_called)
	{
		auth_client_init(); // Call this only once!
		auth_client_init_called = true;
	}
	session = smtp_create_session();
	if (session == NULL)
	{
		elog(LOG,"Unable to send e-mail: smtp_create_session() failed %d",smtp_errno());
		return -1;
	}

	if (log_min_messages <= DEBUG1)
	smtp_set_monitorcb (session, monitor_cb, NULL, 1);

	message = smtp_add_message(session);
	if (message == NULL)
	elog(LOG,"smtp_add_message() failed");

	//smtp_message_reset_status(message);

#ifdef USE_SSL
	if (!smtp_starttls_enable(session, Starttls_ENABLED))
		elog(LOG,"smtp_starttls_enable() failed");
#endif

	/*
	 * Set the host running the SMTP server.  LibESMTP has a default port
	 * number of 587, however this is not widely deployed so the port
	 * is specified as 25 along with the default MTA host.
	 */
	if (!smtp_set_server(session,
					gp_email_smtp_server != NULL && strlen(gp_email_smtp_server) > 0 ? gp_email_smtp_server : "localhost:25"))
	{
		elog(LOG,"Unable to send e-mail: smtp_set_server failed %d",smtp_errno());
		smtp_destroy_session(session);
		return -1;
	}

	smtp_set_eventcb(session, event_cb, NULL);

	/*
	 * Do what's needed at application level to use authentication.
	 */
	authctx = auth_create_context();
	if (authctx == NULL)
	elog(LOG,"auth_create_context() failed %d",smtp_errno());

	if (!auth_set_mechanism_flags(authctx, AUTH_PLUGIN_PLAIN | AUTH_PLUGIN_ANONYMOUS, 0))
	{
		elog(LOG,"Can't auth_set_mechanism_flags");
	}

	auth_set_interact_cb(authctx, authinteract, NULL);

	/*
	 * From what I can tell, we aren't supposed to call auth_set_external_id() directly.
	 * Instead, it gets called automatically based on the information in the client certificate,
	 * which is usually the e-mail address.
	 * Perhaps this might be needed for special circumstances?
	 */
	//if (gp_email_smtp_userid != NULL && gp_email_smtp_userid[0] != '\0')
	//	auth_set_external_id(authctx, gp_email_smtp_userid);

	if (!smtp_auth_set_context(session, authctx))
	{
		elog(LOG,"Can't smtp_auth_set_context");
	}

	/*
	 * Use our callback for X.509 certificate passwords.  If STARTTLS is
	 * not in use or disabled in configure, the following is harmless.
	 */
	smtp_starttls_set_password_cb(tlsinteract, NULL);

	/*
	 * Set the reverse path for the mail envelope.  (NULL is ok)
	 */
	if (strchr(gp_email_from,'<') != NULL)
	{
		/*
		 * Pull out just the e-mail address
		 */
		char * from = pstrdup(strchr(gp_email_from,'<')+1);
		if (strchr(from,'>') != NULL)
		*strchr(from,'>') = '\0';
		if (!smtp_set_reverse_path(message, from))
		elog(LOG,"smtp_set_reverse_path failed %d",smtp_errno());
	}
	else
	if (!smtp_set_reverse_path(message, gp_email_from))
	elog(LOG,"smtp_set_reverse_path failed %d",smtp_errno());

	smtp_set_header(message, "Subject", subject);
	smtp_set_header_option(message, "Subject", Hdr_OVERRIDE, 1);
	smtp_set_header(message, "Message-Id", NULL);

	if (email_priority[0] != '\0' && email_priority[0] != '3') // priority not the default?
	smtp_set_header(message, "X-Priority", email_priority); // set a priority.  1 == highest priority, 5 lowest


	smtp_set_messagecb(message, messagebody_cb, (void *) errorData);

	/* Need a modifiable copy of To list */
	rawstring = pstrdup(gp_email_to);

	/* Parse string into list of identifiers */
	if (!SplitString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		ereport(LOG,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid list syntax for \"gp_email_to\"")));
	}

#if 0
	/*
	 * We want to set a "To:" header.
	 *
	 * Some versions of libesmtp have a bug that only allows this to work for a single
	 * e-mail recipient.  Because of that, I've changed the code to set the "To:" header
	 * in the message body callback, but I'm not sure that is as good as doing this, since I
	 * might not have it formatted the best way.
	 *
	 */
	foreach(l, elemlist)
	{
		char *cur_gp_email_addr = (char *) lfirst(l);

		char *free_form_text = NULL;
		char *email_addr = cur_gp_email_addr;

		/*
		 * RFC 2822 doesn't require recipient headers but a To: header would
		 * be nice to have if not present.
		 *
		 * First string is free-form text which is the real name of the person (not really used)
		 * The second string is the e-mail address.
		 */
		if (strchr(cur_gp_email_addr,'<') != NULL)
		{
			free_form_text = pstrdup(cur_gp_email_addr);
			*strchr(free_form_text,'<') = '\0';
			email_addr = pstrdup(strchr(cur_gp_email_addr,'<')+1);
			if (strchr(email_addr,'>') != NULL)
			*strchr(email_addr,'>') = '\0';
		}

		if (!smtp_set_header(message, "To", free_form_text, email_addr));
		{
			char buf[128];

			elog(LOG, "smtp_set_header failed:  %d  %s  for e-mail %s", smtp_errno(), smtp_strerror(smtp_errno(),
							buf, sizeof buf), cur_gp_email_addr);
		}
	}
#endif

	foreach(l, elemlist)
	{
		char *cur_gp_email_addr = (char *) lfirst(l);

		if (cur_gp_email_addr != NULL && strlen(cur_gp_email_addr) > 0)
		{
			char * to = cur_gp_email_addr;

			/* Recipient must be in RFC2821 format */

			if (strchr(cur_gp_email_addr,'<') != NULL)
			{
				/*
				 * Pull out just the e-mail address
				 */
				to = pstrdup(strchr(cur_gp_email_addr,'<')+1);
				if (strchr(to,'>') != NULL)
				*strchr(to,'>') = '\0';
			}

			recipient = smtp_add_recipient(message, to);

			if (recipient == NULL)
			elog(LOG, "stmp_add_recipient failed for e-mail:  %s", cur_gp_email_addr);

			smtp_dsn_set_notify(recipient, notify);

			success = true;
		}

	}

	if (!success && list_length(elemlist))
	ereport(LOG,
			(errmsg("Could not understand e-mail To: list")));

	list_free(elemlist);
	pfree(rawstring);

	/*
	 * Many e-mail transfer agents don't accept the 8BITMIME extension.  We'd
	 * Like to use it if available, but I don't know how to do that yet.
	 * For now, just don't set it.  The MTA will decide if it is 8BIT based on
	 * the MIME header.
	 */
	/* smtp_8bitmime_set_body(message, E8bitmime_8BITMIME); */

	/* Initiate a connection to the SMTP server and transfer the message. */
	int smtp_ret = smtp_start_session(session);
	if (smtp_ret == 0)
	{
		num_connect_failures++;
		last_connect_failure_ts = time(0);  

		/*
		 * If we get here, we can't talk to the SMTP server at all
		 */
		int smtperr = smtp_errno();
		char buf[128];
		memset(buf, 0, sizeof(buf)); /* in case smtp_strerror fails */
		smtp_strerror(smtperr, buf, sizeof buf);

		elog(LOG, "SMTP server problem %d  %s", smtperr, buf);
	}
	else
	{
                
		/* I would expect this to be always zero here, but just in case it's not */
		if (smtp_errno() != 0)
		{
			int smtperr = smtp_errno();
			char buf[128];
			memset(buf, 0, sizeof(buf)); /* in case smtp_strerror fails */
			smtp_strerror(smtperr, buf, sizeof buf);

			elog(LOG, "SMTP server problem %d  %s", smtperr, buf);
		}
                num_connect_failures = 0;
		/*
		 * Report on the success or otherwise of the mail transfer.
		 * This really only tells the "overall" status, so if an individual e-mail fails,
		 * we won't see it here (even if every one of them fails!).
		 */

		/*
		 * SMTP protocol status codes (see RFCs on SMTP)
		 * status 250 == ok
		 * 200 to 299 are warning or information.
		 * >= 300 are errors
		 */
		status = smtp_message_transfer_status(message);
		elog(status->code == 250 ? DEBUG1 : LOG, "overall transfer status: %d %s", status->code,
				(status->text != NULL) ? status->text : "");

		/*
		 * This should tell us how each e-mail recipient fared.
		 */
		if (!smtp_enumerate_recipients(message, print_recipient_status, NULL))
		{
			elog(LOG,"smtp_enumerate_recipients failed %d",smtp_errno());
		}
	}

	/* Free resources consumed by the program.
	 */
	if (session != NULL)
	smtp_destroy_session(session);
	if (authctx != NULL)
	auth_destroy_context(authctx);

	/* To save on overhead, don't call this more than necessary */
	/* auth_client_exit(); */

	return 0;

}

#endif


int send_alert(const GpErrorData * errorData)
{

	char subject[128];
	bool send_via_email = true;
	char email_priority[2];
	bool send_via_snmp = true;
	char snmp_severity[2];

	static char previous_subject[128];
	pg_time_t current_time;
	static pg_time_t previous_time;
	static GpErrorDataFixFields previous_fix_fields;

	elog(DEBUG2,"send_alert: %s: %s",errorData->error_severity, errorData->error_message);

	/*
	 * SIGPIPE must be ignored, or we will have problems.
	 *
	 * but this is already set in syslogger.c, so we are OK
	 * //pqsignal(SIGPIPE, SIG_IGN);  // already set in syslogger.c
	 *
	 */

	/* Set up a subject line for the alert.  set_alert_severity will limit it to 127 bytes just to be safe. */
	/* Assign a severity and email priority for this alert*/

	set_alert_severity(errorData,
						subject,
						&send_via_email,
						email_priority,
						&send_via_snmp,
						snmp_severity);


	/*
	 * Check to see if we are sending the same message as last time.
	 * This could mean the system is in a loop generating an error over and over,
	 * or the application is just re-doing the same bad request over and over.
	 *
	 * This is pretty crude, as we don't consider loops where we alternate between a small
	 * number of messages.
	 */

	if (strcmp(subject,previous_subject)==0)
	{
		/*
		 * Looks like the same message based on the errMsg, but we need to
		 * investigate further.
		 */
		bool same_message_repeated = true;

		/*
		 * If the message is from a different segDB, consider it a different message.
		 */
		if (errorData->fix_fields.gp_segment_id != previous_fix_fields.gp_segment_id)
			same_message_repeated = false;
		if (errorData->fix_fields.gp_is_primary != previous_fix_fields.gp_is_primary)
			same_message_repeated = false;
		/*
		 * If the message is from a different user, consider it a different message,
		 * unless it is a FATAL, because an application repeatedly sending in a request
		 * that crashes (SIGSEGV) will get a new session ID each time
		 */
		if (errorData->fix_fields.gp_session_id != previous_fix_fields.gp_session_id)
			if (strcmp(errorData->error_severity,"FATAL") != 0)
				same_message_repeated = false;
		/*
		 * Don't consider gp_command_count, because a loop where the application is repeatedly
		 * sending a bad request will have a changing command_count.
		 *
		 * Likewise, the transaction ids will be changing each time, so don't consider them.
		 */

		if (same_message_repeated)
		{
			current_time = (pg_time_t)time(NULL);
			/*
			 * This is the same alert as last time.  Limit us to one repeat alert every 30 seconds
			 * to avoid spamming the sysAdmin's mailbox or the snmp network monitor.
			 *
			 * We don't just turn off the alerting until a different message comes in, because
			 * if enough time has passed, this message might (probably?) refer to a new issue.
			 *
			 * Note that the message will still exist in the log, it's just that we won't
			 * send it via e-mail or snmp notification.
			 */

			if (current_time - previous_time < 30)
			{
				/* Bail out here rather than send the alert. */
				elog(DEBUG2,"Suppressing repeats of this alert messages...");
				return -1;
			}
		}

	}

	strcpy(previous_subject, subject);
	previous_time = (pg_time_t)time(NULL);
	memcpy(&previous_fix_fields,&errorData->fix_fields,sizeof(GpErrorDataFixFields));

#if USE_SNMP
	if (send_via_snmp)
		send_snmp_inform_or_trap(errorData, subject, snmp_severity);
	else 
		elog(DEBUG4,"Not sending via SNMP");
#endif

#ifdef USE_EMAIL
	if (send_via_email)
	{
		send_alert_via_email(errorData, subject, email_priority);
	}
	else
		elog(DEBUG4,"Not sending via e-mail");

#endif

	return 0;
}


static size_t
pg_strnlen(const char *str, size_t maxlen)
{
	const char *p = str;

	while (maxlen-- > 0 && *p)
		p++;
	return p - str;
}

static void move_to_next_chunk(CSVChunkStr * chunkstr,
		const PipeProtoChunk * saved_chunks)
{
	Assert(chunkstr != NULL);
	Assert(saved_chunks != NULL);

	if (chunkstr->chunk != NULL)
		if (chunkstr->p - chunkstr->chunk->data >= chunkstr->chunk->hdr.len)
		{
			/* switch to next chunk */
			if (chunkstr->chunk->hdr.next >= 0)
			{
				chunkstr->chunk = &saved_chunks[chunkstr->chunk->hdr.next];
				chunkstr->p = chunkstr->chunk->data;
			}
			else
			{
				/* no more chunks */
				chunkstr->chunk = NULL;
				chunkstr->p = NULL;
			}
		}
}

static char *
get_str_from_chunk(CSVChunkStr *chunkstr, const PipeProtoChunk *saved_chunks)
{
	int wlen = 0;
	int len = 0;
	char * out = NULL;

	Assert(chunkstr != NULL);
	Assert(saved_chunks != NULL);

	move_to_next_chunk(chunkstr, saved_chunks);

	if (chunkstr->p == NULL)
	{
		return strdup("");
	}

	len = chunkstr->chunk->hdr.len - (chunkstr->p - chunkstr->chunk->data);

	/* Check if the string is an empty string */
	if (len > 0 && chunkstr->p[0] == '\0')
	{
		chunkstr->p++;
		move_to_next_chunk(chunkstr, saved_chunks);

		return strdup("");
	}

	if (len == 0 && chunkstr->chunk->hdr.next >= 0)
	{
		const PipeProtoChunk *next_chunk =
				&saved_chunks[chunkstr->chunk->hdr.next];
		if (next_chunk->hdr.len > 0 && next_chunk->data[0] == '\0')
		{
			chunkstr->p++;
			move_to_next_chunk(chunkstr, saved_chunks);
			return strdup("");
		}
	}

	wlen = pg_strnlen(chunkstr->p, len);

	if (wlen < len)
	{
		// String all contained in this chunk
		out = malloc(wlen + 1);
		memcpy(out, chunkstr->p, wlen + 1); // include the null byte
		chunkstr->p += wlen + 1; // skip to start of next string.
		return out;
	}

	out = malloc(wlen + 1);
	memcpy(out, chunkstr->p, wlen);
	out[wlen] = '\0';
	chunkstr->p += wlen;

	while (chunkstr->p)
	{
		move_to_next_chunk(chunkstr, saved_chunks);
		if (chunkstr->p == NULL)
			break;
		len = chunkstr->chunk->hdr.len - (chunkstr->p - chunkstr->chunk->data);

		wlen = pg_strnlen(chunkstr->p, len);

		/* Write OK, don't forget to account for the trailing 0 */
		if (wlen < len)
		{
			// Remainder of String all contained in this chunk
			out = realloc(out, strlen(out) + wlen + 1);
			strncat(out, chunkstr->p, wlen + 1); // include the null byte

			chunkstr->p += wlen + 1; // skip to start of next string.
			return out;
		}
		else
		{
			int newlen = strlen(out) + wlen;
			out = realloc(out, newlen + 1);
			strncat(out, chunkstr->p, wlen);
			out[newlen] = '\0';

			chunkstr->p += wlen;
		}
	}

	return out;
}

#ifdef USE_EMAIL

/*
 *  The message is read a line at a time and the newlines converted
 *  to \r\n.  Unfortunately, RFC 822 states that bare \n and \r are
 *  acceptable in messages and that individually they do not constitute a
 *  line termination.  This requirement cannot be reconciled with storing
 *  messages with Unix line terminations.  RFC 2822 rescues this situation
 *  slightly by prohibiting lone \r and \n in messages.
 *
 */
static void add_to_message(char * message, const char * newstr_in)
{
	const char * newstr = newstr_in;
	char * p;
	if (newstr == NULL)
		return;

	/* Drop any leading \n characters:  Not sure what to do with them */
	while (*newstr == '\n')
		newstr++;

	/* Scan for \n, and convert to \r\n */
	while ((p = strchr(newstr,'\n')) != NULL)
	{
		/* Don't exceed 900 chars added to this line, so total line < 1000 */
		if (p - newstr >= 900)
		{
			strncat(message, newstr, 898);
			strcat(message, "\r\n\t");
			newstr += 898;
		}
		else if (p - newstr >=2 && *(p-1) != '\r')
		{
			strncat(message, newstr, p - newstr);
			strcat(message, "\r\n\t");
			newstr = p+1;
		}
		else
		{
			strncat(message, newstr, p - newstr + 1);
			newstr = p+1;
		}
	}
	strcat(message, newstr);

}

static const char *
messagebody_cb(void **buf, int *len, void *arg)
{
#define MAX_MESSAGE_BODY 8192
	const GpErrorData *errorData = (GpErrorData *) arg;
	char * message = NULL;
	static int alreadyreturned = 0;

	if (*buf == NULL)
	{
		alreadyreturned = 0;
		if (len == NULL)
			return NULL;

		*buf = malloc(MAX_MESSAGE_BODY);
		message = ((char *)(*buf));
		message[0] = '\0';
	}

	if (len == NULL)
	{
		alreadyreturned = 0;
		message = ((char *)(*buf));
		message[0] = '\0';
	    return NULL;
	}

	message = ((char *)(*buf));

	if (alreadyreturned == 0)
	{
		char  lineno[16]; 

		pg_ltoa(errorData->fix_fields.error_fileline,lineno);

		/* Perhaps better to use text/html ? */

		strcpy(message, "From: ");
		strcat(message, gp_email_from);
		strcat(message,"\r\n");
		strcat(message, "To: ");
		strcat(message,gp_email_to);
		strcat(message,"\r\n");

		strcat(message,
			//"Return-Path: <cmcdevitt@greenplum.com>\r\n"
			//"Subject: LibESMTP test mail\r\n"
			"MIME-Version: 1.0\r\n"
			"Content-Type: text/plain;\r\n"
			"  charset=utf-8\r\n"
			"Content-Transfer-Encoding: 8bit\r\n\r\n");

		/* Lines must be < 1000 bytes long for 7bit or 8bit transfer-encoding */
		/* Perhaps use base64 encoding instead?  Or just wrap them? */

		/* How to identify which system is sending the alert?  Perhaps our hostname and port is good enough? */
		strcat(message,"Alert from GPDB system ");
		{
			char myhostname[255];	/* gethostname usually is limited to 65 chars out, but make this big to be safe */
			char portnum[16];
			myhostname[0] = '\0';


			if (gethostname(myhostname, sizeof(myhostname)) == 0)
			{
				strcat(message,myhostname);
				strcat(message," on port ");
				pg_ltoa(PostPortNumber,portnum);
				strcat(message,portnum);
			}
		}
		strcat(message,":\r\n\r\n");
		if (errorData->username != NULL &&  errorData->databasename != NULL &&
				strlen(errorData->username)>0 && strlen(errorData->databasename)>0)
		{
			strcat(message, errorData->username);
			if (errorData->remote_host != NULL && strlen(errorData->remote_host) > 0)
			{
				if (strcmp(errorData->remote_host,"[local]")==0)
					strcat(message, " logged on locally from master node");
				else
				{
					strcat(message," logged on from host ");
					strcat(message,errorData->remote_host);
				}
			}
			strcat(message, " connected to database ");
			strcat(message, errorData->databasename);
			strcat(message, "\r\n");

		}
		if (errorData->fix_fields.omit_location != 't')
		{
			if (errorData->fix_fields.gp_segment_id != -1)
			{
				char segid[16];
				pg_ltoa(errorData->fix_fields.gp_segment_id,segid);
				strcat(message,"Error occurred on segment ");
				strcat(message,segid);
			}
			else
				strcat(message,"Error occurred on master segment");
			strcat(message, "\r\n");
		}
		strcat(message, "\r\n");

		strcat(message, errorData->error_severity);
		strcat(message, ": ");
		if (errorData->sql_state != NULL && pg_strnlen(errorData->sql_state,5)>4 &&
				strncmp(errorData->sql_state,"XX100",5)!=0 &&
				strncmp(errorData->sql_state,"00000",5)!=0)
		{
			strcat(message, "(");
			strncat(message, errorData->sql_state, 5);
			strcat(message, ") ");
		}
		add_to_message(message, errorData->error_message);
		strcat(message, "\r\n");
		strcat(message, "\r\n");

		if (errorData->error_detail != NULL &&strlen(errorData->error_detail) > 0)
		{
			strcat(message, _("DETAIL:  "));
			add_to_message(message, errorData->error_detail);
			strcat(message, "\r\n");
		}
		if (errorData->error_hint != NULL &&strlen(errorData->error_hint) > 0)
		{
			strcat(message, _("HINT:  "));
			add_to_message(message, errorData->error_hint);
			strcat(message, "\r\n");
		}
		if (errorData->internal_query != NULL &&strlen(errorData->internal_query) > 0)
		{
			strcat(message, _("QUERY:  "));
			add_to_message(message, errorData->internal_query);
			strcat(message, "\r\n");
		}
		if (errorData->error_context != NULL && strlen(errorData->error_context) > 0)
		{
			strcat(message, _("CONTEXT:  "));
			add_to_message(message, errorData->error_context);
			strcat(message, "\r\n");
		}
		if (errorData->fix_fields.omit_location != 't')
		{
			if (errorData->error_filename != NULL && strlen(errorData->error_filename) > 0)
			{
				strcat(message,  _("LOCATION:  "));

				if (errorData->error_func_name && strlen(errorData->error_func_name) > 0)
				{
					strcat(message, errorData->error_func_name);
					strcat(message, ", ");
				}

				strcat(message, errorData->error_filename);
				strcat(message, ":");
				strcat(message, lineno);
				strcat(message, "\r\n");
			}
			if (errorData->stacktrace != NULL && strlen(errorData->stacktrace) > 0)
			{
				strcat(message, "STACK TRACE:\r\n\t");
				add_to_message(message, errorData->stacktrace);
				strcat(message, "\r\n");
			}
		}
		if (errorData->debug_query_string != NULL &&strlen(errorData->debug_query_string) > 0)
		{
			strcat(message, _("STATEMENT:  "));
			add_to_message(message, errorData->debug_query_string);
			strcat(message, "\r\n");
		}

	}

	if (alreadyreturned < strlen(message))
	{
		char * p;
		message += alreadyreturned;
		if ((p = strchr(message,'\n')) != NULL)
		{
			*len = p - message + 1;
			alreadyreturned += *len;
		}
		else if (strlen(message)>0)
		{
			*len = strlen(message);
			alreadyreturned += *len;
		}
		else
		{
			*len = 0;
			message = NULL;
		}
	}
	else
	{
		*len = 0;
		return NULL;
	}

	return message;
}

/* Callback to print the recipient status */
static void print_recipient_status(smtp_recipient_t recipient,
		const char *mailbox, void *arg  __attribute__((unused)))
{
	const smtp_status_t *status;

	status = smtp_recipient_status(recipient);
	/*
	 * SMTP protocol status codes (see RFCs on SMTP)
	 * status 250 == ok
	 * 200 to 299 are warning or information.
	 * >= 300 are errors
	 */
	if (status->code != 0 && status->code != 250)
	{
		elog(LOG, "recipient status: %s: %d %s", mailbox, status->code, status->text);
	}
}

static int authinteract(auth_client_request_t request, char **result,
		int fields, void *arg)
{
	int i;
	
	elog(DEBUG2, "authinteract called");

	/* SASL auth requests */
	for (i = 0; i < fields; i++)
	{
		if (request[i].flags & AUTH_PASS)
		{
			elog(DEBUG1, "authinteract asking for password: %s  %s%s",request[i].name, request[i].prompt,(request[i].flags & AUTH_CLEARTEXT) ? " (not encrypted)"
					: "");
			result[i] = gp_email_smtp_password;
		}
		else if (request[i].flags & AUTH_USER)
		{
			elog(DEBUG1, "authinteract asking for username: %s  %s%s",request[i].name, request[i].prompt,(request[i].flags & AUTH_CLEARTEXT) ? " (not encrypted)"
					: "");
			result[i] = gp_email_smtp_userid;

		}
		else if (request[i].flags & AUTH_REALM)
		{
			elog(LOG, "authinteract asking for realm: %s  %s%s",request[i].name, request[i].prompt,(request[i].flags & AUTH_CLEARTEXT) ? " (not encrypted)"
					: "");
			result[i] = "greenplum.com";

		}
		else if (result[i] == NULL)
		{
			return 0;
		}
	}

	return 1;
}

static int tlsinteract(char *buf, int buflen, int rwflag  __attribute__((unused)), void *arg  __attribute__((unused)))
{

	elog(DEBUG1, "tlsinteract asking for tls password");

	strcpy(buf, gp_email_smtp_password);  // This isn't right... We need the X.509 certificate's password, I think
	return strlen(buf);
}

static int handle_invalid_peer_certificate(long vfy_result)
{
	const char *k = "rare error";
	switch (vfy_result)
	{
	case X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT:
		k = "X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT";
		break;
	case X509_V_ERR_UNABLE_TO_GET_CRL:
		k = "X509_V_ERR_UNABLE_TO_GET_CRL";
		break;
	case X509_V_ERR_UNABLE_TO_DECRYPT_CERT_SIGNATURE:
		k = "X509_V_ERR_UNABLE_TO_DECRYPT_CERT_SIGNATURE";
		break;
	case X509_V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE:
		k = "X509_V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE";
		break;
	case X509_V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY:
		k = "X509_V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY";
		break;
	case X509_V_ERR_CERT_SIGNATURE_FAILURE:
		k = "X509_V_ERR_CERT_SIGNATURE_FAILURE";
		break;
	case X509_V_ERR_CRL_SIGNATURE_FAILURE:
		k = "X509_V_ERR_CRL_SIGNATURE_FAILURE";
		break;
	case X509_V_ERR_CERT_NOT_YET_VALID:
		k = "X509_V_ERR_CERT_NOT_YET_VALID";
		break;
	case X509_V_ERR_CERT_HAS_EXPIRED:
		k = "X509_V_ERR_CERT_HAS_EXPIRED";
		break;
	case X509_V_ERR_CRL_NOT_YET_VALID:
		k = "X509_V_ERR_CRL_NOT_YET_VALID";
		break;
	case X509_V_ERR_CRL_HAS_EXPIRED:
		k = "X509_V_ERR_CRL_HAS_EXPIRED";
		break;
	case X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD:
		k = "X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD";
		break;
	case X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD:
		k = "X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD";
		break;
	case X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD:
		k = "X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD";
		break;
	case X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD:
		k = "X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD";
		break;
	case X509_V_ERR_OUT_OF_MEM:
		k = "X509_V_ERR_OUT_OF_MEM";
		break;
	case X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT:
		k = "X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT";
		break;
	case X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN:
		k = "X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN";
		break;
	case X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY:
		k = "X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY";
		break;
	case X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE:
		k = "X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE";
		break;
	case X509_V_ERR_CERT_CHAIN_TOO_LONG:
		k = "X509_V_ERR_CERT_CHAIN_TOO_LONG";
		break;
	case X509_V_ERR_CERT_REVOKED:
		k = "X509_V_ERR_CERT_REVOKED";
		break;
	case X509_V_ERR_INVALID_CA:
		k = "X509_V_ERR_INVALID_CA";
		break;
	case X509_V_ERR_PATH_LENGTH_EXCEEDED:
		k = "X509_V_ERR_PATH_LENGTH_EXCEEDED";
		break;
	case X509_V_ERR_INVALID_PURPOSE:
		k = "X509_V_ERR_INVALID_PURPOSE";
		break;
	case X509_V_ERR_CERT_UNTRUSTED:
		k = "X509_V_ERR_CERT_UNTRUSTED";
		break;
	case X509_V_ERR_CERT_REJECTED:
		k = "X509_V_ERR_CERT_REJECTED";
		break;
	}

	/*
	 * These two errors come fgpdb-2010-02-18_184837.csvrom using self-signed certificates.  We don't care about that.
	 */
	if (vfy_result != X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY &&
		vfy_result != X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN)
		elog(DEBUG1, "SMTP_EV_INVALID_PEER_CERTIFICATE: %ld: %s", vfy_result, k);

	return 1; /* Accept the problem and try to continue  */
}

/*
 * We currently just return 1 to keep libesmtp happy
 * Will turn this into a proper event handler some day
 */
static void event_cb(smtp_session_t session, int event_no, void *arg, ...)
{
	va_list alist;
	int *ok;

	va_start(alist, arg);

	switch (event_no)
	{

	case SMTP_EV_WEAK_CIPHER:
		{
			long bits;

			bits = va_arg(alist, long);
			ok = va_arg(alist, int*);
			elog(DEBUG3,"WEAK_CIPHER bits=%ld",bits);
			*ok = 1;
		}
		break;

	case SMTP_EV_INVALID_PEER_CERTIFICATE:
		{
			long vfy_result;
			vfy_result = va_arg(alist, long);
			ok = va_arg(alist, int*);
			*ok = handle_invalid_peer_certificate(vfy_result);
		}
		break;

	case SMTP_EV_NO_PEER_CERTIFICATE:
		{
			ok = va_arg(alist, int*);
			elog(DEBUG3,"NO PEER CERT");
			*ok = 1;
		}
		break;
	case SMTP_EV_WRONG_PEER_CERTIFICATE:
		{
			ok = va_arg(alist, int*);
			elog(DEBUG2,"SMTP_EV_WRONG_PEER_CERTIFICATE");
			*ok = 1;
		}
		break;
	case SMTP_EV_NO_CLIENT_CERTIFICATE:
		{
			ok = va_arg(alist, int*);
			elog(DEBUG2,"NO CLIENT CERT");
			*ok = 1;
		}
		break;

	case SMTP_EV_STARTTLS_OK:
		elog(DEBUG2,"SMTP_EV_STARTTLS_OK");
		break;
	case SMTP_EV_CONNECT:
		elog(DEBUG1,"SMTP_EV_CONNECT");
		break;
	case SMTP_EV_MAILSTATUS:
	{
		const smtp_status_t *status = NULL;

		smtp_message_t msg;
		const char * rev_path;
		rev_path = va_arg(alist, const char*);
		msg = va_arg(alist, smtp_message_t);

		if (msg != NULL)
		status = smtp_reverse_path_status (msg);

		if (status && status->code != 0 && status->code != 250 && status->text != NULL)
			elog(LOG,"EV_MAILSTATUS: SMTP server reports a problem %d:  %s:  %s", status->code, rev_path,  status->text);
	}
	break;
	case SMTP_EV_RCPTSTATUS:
#if 0
	{

		/*
		 * Debugging code... Don't need this in production use, but useful during development.
		 */
		const smtp_status_t *status = NULL;

		smtp_mailbox_t mbox;
		mbox = va_arg(alist, smtp_mailbox_t);
		smtp_recipient_t rcpt;
		rcpt = va_arg(alist, smtp_recipient_t);
		if (rcpt != NULL)
			status = smtp_recipient_status (rcpt);

		if (status && status->code != 0 && status->code != 250 && status->text != NULL)
			elog(LOG,"SMTP server reports a  problem %d: %s", status->code, status->text);
	}
#endif
		break;
	case SMTP_EV_MESSAGEDATA:
		break;
	case SMTP_EV_MESSAGESENT:
	{
		const smtp_status_t *status = NULL;

		smtp_message_t msg;
		msg = va_arg(alist, smtp_message_t);
		if (msg != NULL)
			status = smtp_message_transfer_status (msg);

		if (status && status->code != 0 && status->code != 250 && status->text != NULL)
			elog(LOG,"SMTP server reports a messagesent problem %d: %s", status->code, status->text);
		else
			elog(DEBUG2,"EV_MESSAGESENT");
	}
	break;
	case SMTP_EV_DISCONNECT:
		elog(DEBUG2, "EV_DISCONNECT");
		break;

	case SMTP_EV_EXTNA_8BITMIME:
		elog(DEBUG1, "SMTP_EV_EXTNA_8BITMIME");
		break;

	case SMTP_EV_EXTNA_DSN:
		elog(DEBUG1, "SMTP_EV_EXTNA_DSN");
		break;

	default:
		elog(LOG, "SMTP_EV_%d",event_no);
		break;

	}

	va_end(alist);
}

/*
 * This routine is strictly for debugging... It dumps out our interaction with the SMTP server.
 */
static void
monitor_cb (const char *buf, int buflen, int writing, void *arg)
{
  elog(DEBUG1, "SMTP %s: %.*s",(writing == SMTP_CB_HEADERS) ? "H" : writing ? "C" : "S", buflen, buf);
}


#endif

static bool
SplitString(char *rawstring, char delimiter,
					  List **namelist)
{
	char	   *nextp = rawstring;
	bool		done = false;

	*namelist = NIL;

	while (isspace((unsigned char) *nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new address. */
	do
	{
		char	   *curname;
		char	   *endp;

		curname = nextp;
		while (*nextp && *nextp != delimiter)
			nextp++;
		endp = nextp;
		if (curname == nextp)
			return false;	/* empty unquoted name not allowed */



		while (isspace((unsigned char) *nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == delimiter)
		{
			nextp++;
			while (isspace((unsigned char) *nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/*
		 * Finished isolating current name --- add it to list
		 */
		*namelist = lappend(*namelist, curname);

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}
