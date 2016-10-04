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

#include "access/pxfutils.h"

#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include <net/if.h>
#include <ifaddrs.h>
#include <sys/socket.h>
#include <netdb.h>

/* Wrapper for libchurl */
static void process_request(ClientContext* client_context, char *uri);

/* checks if two ip strings are equal */
bool are_ips_equal(char *ip1, char *ip2)
{
	if ((ip1 == NULL) || (ip2 == NULL))
		return false;
	return (strcmp(ip1, ip2) == 0);
}

/* override port str with given new port int */
void port_to_str(char **port, int new_port)
{
	char tmp[10];

	if (!port)
		elog(ERROR, "unexpected internal error in pxfutils.c");
	if (*port)
		pfree(*port);

	Assert((new_port <= 65535) && (new_port >= 1)); /* legal port range */
	pg_ltoa(new_port, tmp);
	*port = pstrdup(tmp);
}

/*
 * get_hdfs_location_from_filespace
 *
 * Get hdfs location from pg_filespace_entry
 * The returned path needs to be pfreed by the caller.
 */
void get_hdfs_location_from_filespace(char** path)
{
	Assert(NULL != path);
	Oid dtsoid = get_database_dts(MyDatabaseId);
	GetFilespacePathForTablespace(dtsoid, path);

	Assert(NULL != *path);
	Assert(strlen(*path) < FilespaceLocationBlankPaddedWithNullTermLen);

	elog(DEBUG2, "found hdfs location is %s", *path);
}

/*
 * call_rest
 *
 * Creates the REST message and sends it to the PXF service located on
 * <hadoop_uri->host>:<hadoop_uri->port>
 */
void
call_rest(GPHDUri *hadoop_uri,
		  ClientContext *client_context,
		  char *rest_msg)
{
	StringInfoData request;
	initStringInfo(&request);

	appendStringInfo(&request, rest_msg,
								hadoop_uri->host,
								hadoop_uri->port,
								PXF_SERVICE_PREFIX,
								PXF_VERSION);

	/* send the request. The response will exist in rest_buf.data */
	process_request(client_context, request.data);
	pfree(request.data);
}

/*
 * Wrapper for libchurl
 */
static void process_request(ClientContext* client_context, char *uri)
{
	size_t n = 0;
	char buffer[RAW_BUF_SIZE];

	print_http_headers(client_context->http_headers);
	client_context->handle = churl_init_download(uri, client_context->http_headers);
	memset(buffer, 0, RAW_BUF_SIZE);
	resetStringInfo(&(client_context->the_rest_buf));

	/*
	 * This try-catch ensures that in case of an exception during the "communication with PXF and the accumulation of
	 * PXF data in client_context->the_rest_buf", we still get to terminate the libcurl connection nicely and avoid
	 * leaving the PXF server connection hung.
	 */
	PG_TRY();
	{
		/* read some bytes to make sure the connection is established */
		churl_read_check_connectivity(client_context->handle);
		while ((n = churl_read(client_context->handle, buffer, sizeof(buffer))) != 0)
		{
			appendBinaryStringInfo(&(client_context->the_rest_buf), buffer, n);
			memset(buffer, 0, RAW_BUF_SIZE);
		}
		churl_cleanup(client_context->handle, false);
	}
	PG_CATCH();
	{
		if (client_context->handle)
			churl_cleanup(client_context->handle, true);
		PG_RE_THROW();
	}
	PG_END_TRY();


}

/*
 * Finds ip address of any available loopback interface(ipv4/ipv6)
 *
 */
char* get_loopback_ip_addr() {
	struct ifaddrs *ifaddr, *ifa;
	int family, s, n;
	char host[NI_MAXHOST];
	char *loopback_addr;

	if (getifaddrs(&ifaddr) == -1) {
		elog(ERROR, "Unable to obtain list of network interfaces.");
	}

	for (ifa = ifaddr, n = 0; ifa != NULL; ifa = ifa->ifa_next, n++) {
		if (ifa->ifa_addr == NULL)
			continue;

		family = ifa->ifa_addr->sa_family;

		if (family == AF_INET || family == AF_INET6) {
			s = getnameinfo(ifa->ifa_addr,
					(family == AF_INET) ?
							sizeof(struct sockaddr_in) :
							sizeof(struct sockaddr_in6), host, NI_MAXHOST,
					NULL, 0, NI_NUMERICHOST);
			if (s != 0) {
				elog(WARNING, "Unable to get name information for interface, getnameinfo() failed: %s\n", gai_strerror(s));
			}

			//get loopback interface
			if (ifa->ifa_flags & IFF_LOOPBACK) {
				loopback_addr = host;
				break;
			}
		}
	}

	freeifaddrs(ifaddr);

	if (loopback_addr == NULL)
		elog(ERROR, "Unable to get loop back address");

	return loopback_addr;
}

