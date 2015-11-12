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

#include "access/pxfcomutils.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

/*
 * Test if a server at host:port is up
 * return true if up, false if down
 */
bool ping(char* host, char *port)
{
	int rc;
	struct in6_addr serveraddr;
	struct addrinfo hints, *res = NULL;
	int    sd = -1;
	bool up = true;
	
	/* cover the case where the host is an IP address in IPv4 or IPv6 protocol */ 
	memset(&hints, 0, sizeof(hints));
	hints.ai_flags    = AI_NUMERICSERV;
	hints.ai_family   = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;	
	/*in case the host is a numeric ip*/
	rc = inet_pton(AF_INET, host, &serveraddr);
	if (rc == 1)    /* valid IPv4 text address? */
	{
		hints.ai_family = AF_INET;
		hints.ai_flags |= AI_NUMERICHOST;
	}
	else
	{
		rc = inet_pton(AF_INET6, host, &serveraddr);
		if (rc == 1) /* valid IPv6 text address? */
		{
			
            hints.ai_family = AF_INET6;
            hints.ai_flags |= AI_NUMERICHOST;
		}
	}
	
	/*  
	 * do-while enbles us to have cleaning code in one place, even if we fail
	 * in the first or second API call. There are 3 calls in the following sequence
	 */
	do 
	{
		rc = getaddrinfo(host, port, &hints, &res);
		if (rc != 0)
		{
			up = false; 
			break;
		}
			
		sd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		if (sd < 0)
		{
			up = false;
			break;
		}

		rc = connect(sd, res->ai_addr, res->ai_addrlen); 
		if (rc < 0)
			up = false;
	}
	while (false);
	
	if (sd != -1)
		close(sd);	
	if (res != NULL)
		freeaddrinfo(res); 
	
	return up;
}
