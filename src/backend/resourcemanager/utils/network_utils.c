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

#include "utils/network_utils.h"
#include "utils/memutilities.h"
#include "miscadmin.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>

#include "dynrm.h"
/*
 * Global variables for socket connection pool
 */
HASHTABLEData ResolvedHostnames;	/* All resolved hostname's address info.  */
HASHTABLEData ActiveConnections;	/* All currently active connections.	  */

static void cleanupSocketConnectionPool(int code, Datum arg);

uint64_t gettime_microsec(void)
{
    static struct timeval t;
    gettimeofday(&t,NULL);
    return 1000000ULL * t.tv_sec + t.tv_usec;
}

int getHostIPV4AddressesByHostNameAsString(MCTYPE 	 		context,
										   const char 	   *hostname,
										   SimpStringPtr 	ohostname,
										   List   		  **addresses)
{
	Assert(hostname != NULL);
	Assert(ohostname != NULL);
	Assert(addresses != NULL);

	char ipstr[32];
	struct hostent  *hent = NULL;
	*addresses = NULL;
	/* Try to resolve this host by hostname. */

	for ( int i = 0 ; i < NETWORK_RETRY_TIMES ; ++i )
	{
		hent = gethostbyname(hostname);
		if( hent != NULL )
		{
			break;
		}
		else if ( h_errno != TRY_AGAIN )
		{
			write_log("Failed to call gethostbyname() to get host %s, %s",
					  hostname,
					  hstrerror(h_errno));
			break;
		}
		pg_usleep(NETWORK_RETRY_SLEEP_US);
	}

	if ( hent == NULL )
	{
		write_log("Failed to resolve host %s.", hostname);
		return SYSTEM_CALL_ERROR;
	}

	setSimpleStringNoLen(ohostname, hent->h_name);

	if ( hent->h_addrtype != AF_INET )
	{
		return FUNC_RETURN_OK; /* No IPv4 addresses. addresses is set NULL. */
	}

	/* This switch is to support List operation */
	MEMORY_CONTEXT_SWITCH_TO(context)

	for ( char **paddr = hent->h_addr_list ; *paddr != NULL ; paddr++ )
	{

		inet_ntop(hent->h_addrtype, *paddr, ipstr, sizeof(ipstr));

		int newaddrlen = strlen(ipstr);
		AddressString newaddr = (AddressString)
								rm_palloc0(context,
										   __SIZE_ALIGN64(
											   offsetof(AddressStringData, Address) +
											   newaddrlen + 1));
		newaddr->Length = newaddrlen;
		memcpy(newaddr->Address, ipstr, newaddrlen+1);
		*addresses = lappend(*addresses, (void *)newaddr);
	}

	MEMORY_CONTEXT_SWITCH_BACK

	return FUNC_RETURN_OK;
}

void freeHostIPV4AddressesAsString(MCTYPE context, List **addresses)
{
	Assert( addresses != NULL );

	MEMORY_CONTEXT_SWITCH_TO(context)

	while( *addresses != NULL ) {
		rm_pfree(context, lfirst(list_head(*addresses)));
		*addresses = list_delete_first(*addresses);
	}

	MEMORY_CONTEXT_SWITCH_BACK
}

int getLocalHostName(SimpStringPtr hostname)
{
	static int AppendSize = 2;
	/*
	 * Call gethostname() to read hostname, however, the hostname string array
	 * is fixed, therefore there is a possibility that the actual host name is
	 * longer than the pre-created array.
	 *
	 * gethostname() does not return -1 and does not set errno to ENAMETOOLONG
	 * in MACOS ( I did not test in linux yet ). So the logic here to judge if
	 * complete hostname string is returned is to check if we have more than 2
	 * \0 values at the very end of the array.
	 */
	SelfMaintainBufferData  buffer;
	int						sysres  = 0;
	int						res		= FUNC_RETURN_OK;

	initializeSelfMaintainBuffer(&buffer, hostname->Context);

	for ( int i = 0 ; i < 4096/AppendSize ; ++i ) {
		prepareSelfMaintainBuffer(&buffer, AppendSize, true);
		sysres = gethostname(buffer.Buffer, buffer.Size);
		if ( sysres == 0 && buffer.Buffer[buffer.Size-2] == '\0')
			break;
		jumpforwardSelfMaintainBuffer(&buffer, AppendSize);
	}

	if ( sysres != 0 ) {
		res = UTIL_NETWORK_TOOLONG_HOSTNAME;
	}
	else {
		/* Copy out the hostname string. */
		setSimpleStringNoLen(hostname, buffer.Buffer);
	}
	destroySelfMaintainBuffer(&buffer);
	return res;
}

int getLocalHostAllIPAddressesAsStrings(DQueue addresslist)
{
	int		 res	   = FUNC_RETURN_OK;
	struct ifaddrs *ifaddr, *ifa;
	int family, s, n;
	char host[NI_MAXHOST];

	if ( getifaddrs(&ifaddr) == -1 )
	{
		elog(ERROR, "Failed to get interface addresses when calling getifaddrs(), "
					"errno %d",
					errno);
	}

	for (ifa = ifaddr, n = 0; ifa != NULL; ifa = ifa->ifa_next, n++)
	{
		if ( ifa->ifa_addr == NULL )
		{
			continue;
		}

		family = ifa->ifa_addr->sa_family;

		if ( family == AF_INET )
		{
			s = getnameinfo(ifa->ifa_addr,
							sizeof(struct sockaddr_in),
							host,
							NI_MAXHOST,
							NULL, 0, NI_NUMERICHOST);
			if ( s != 0 )
			{
				elog(WARNING, "Fail to call getnameinfo(), err %s",
							  gai_strerror(s	));
				continue;
			}
			HostAddress newaddr = NULL;
			newaddr = createHostAddressAsStringFromIPV4AddressStr(addresslist->Context,
																  host);
			insertDQueueTailNode(addresslist, newaddr);

			elog(LOG, "Resource manager discovered local host IPv4 address %s",
					  ((AddressString)(newaddr->Address))->Address);
		}
	}

	freeifaddrs(ifaddr);

	return res;
}

HostAddress createHostAddressAsStringFromIPV4AddressStr(MCTYPE context, const char *addr)
{
	static uint32_t addrlen = 0;
	HostAddress 	result = NULL;
	int				resultsize = 0;

	result = rm_palloc0(context, sizeof(HostAddressData));
	result->Attribute.Offset  = 0;
	result->Attribute.Mark   |= HOST_ADDRESS_CONTENT_STRING;

	addrlen = strlen(addr);
	resultsize = __SIZE_ALIGN64(offsetof(AddressStringData, Address) +
								addrlen + 1 );
	AddressString straddr = rm_palloc0(context, resultsize);

	straddr->Length = addrlen;
	strcpy(straddr->Address, addr);
	result->Address = (char *)straddr;
	result->AddressSize = resultsize;

	return result;
}

void freeHostAddress(MCTYPE context, HostAddress addr)
{
	rm_pfree(context, addr->Address);
	rm_pfree(context, addr);
}

bool AddressStringComp(AddressString addr1, AddressString addr2)
{
	if ( addr1 == NULL || addr2 == NULL )
		return false;

	if ( addr1->Length != addr2->Length )
		return false;

	return strcmp(addr1->Address, addr2->Address) == 0;
}

int sendWithRetry(int fd, char *buff, int size, bool checktrans)
{
	int nsent = 0;
	int res   = 0;
retry:
	res = send(fd, buff+nsent, size-nsent, 0);
	if ( res > 0 ) {
		nsent += res;
		if ( nsent < size )
			goto retry;
		else
			return FUNC_RETURN_OK;
	}
	else {
		if ( checktrans && QueryCancelPending ) {
			return TRANSCANCEL_INPROGRESS;
		}
		else if ( errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR ) {
			goto retry;
		}
	}

	/* Not acceptable error. */
	return SYSTEM_CALL_ERROR;
}

int recvWithRetry(int fd, char *buff, int size, bool checktrans)
{
	int nrecv = 0;
	int res   = 0;
retry:
	res = recv(fd, buff+nrecv, size-nrecv, 0);
	if ( res > 0 ) {
		nrecv += res;
		if ( nrecv < size )
			goto retry;
		else
			return FUNC_RETURN_OK;
	}
	else if ( res == 0 ) {
		return UTIL_NETWORK_REMOTE_CLOSED;
	}
	else {
		if ( checktrans && QueryCancelPending ) {
			return TRANSCANCEL_INPROGRESS;
		}
		else if ( res == -1 &&
			      (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR)) {
			/* In case, not getting expected data yet in non-block mode. */
			goto retry;
		}
	}

	/* Not acceptable error. */
	return SYSTEM_CALL_ERROR;
}

/*
 * Set the connection as non-blocked mode.
 */
int setConnectionNonBlocked(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags == -1)
	{
		write_log("Failed to call fcntl GETFL, fd %d (errno %d)", fd, errno);
		return SYSTEM_CALL_ERROR;
	}
	flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	if (flags == -1)
	{
		write_log("Failed to call fcntl SETFL, fd %d (errno %d)", fd, errno);
		return SYSTEM_CALL_ERROR;
	}
	return FUNC_RETURN_OK;
}

/*
 * A wrapper for getting one socket connection to server.
 *
 * address[in]			The address to connect.
 * port[in] 			The port number.
 * clientfd[out]		The fd of connection.
 *
 * Return:
 * FUNC_RETURN_OK					Succeed.
 * UTIL_NETWORK_FAIL_CREATESOCKET. 	Fail to call socket().
 * UTIL_NETWORK_FAIL_GETHOST. 		Fail to call gethostbyname().
 * UTIL_NETWORK_FAIL_CONNECT. 		Fail to call connect().
 */
int connectToServerRemote(const char *address, uint16_t port, int *clientfd)
{
	int					fd		= -1;
	int 		    	sockres = 0;
	struct sockaddr_in 	server_addr;

	*clientfd = -1;

	AddressString resolvedaddr = getAddressStringByHostName(address);
	if ( resolvedaddr == NULL )
	{
		write_log("Failed to get host by name %s for connecting to a remote "
				  "socket server %s:%d",
				  address,
				  address,
				  port);
		return UTIL_NETWORK_FAIL_GETHOST;
	}

	if ( rm_enable_connpool )
	{
		/* Try to get an alive connection from connection pool. */
		fd = fetchAliveSocketConnection(address, resolvedaddr, port);
	}

	if ( fd != -1 )
	{
		*clientfd = fd;
		return FUNC_RETURN_OK;
	}

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if ( fd < 0 )
	{
		write_log("Failed to open socket for connecting remote socket server "
				  "(errno %d)",
				  errno);
		return UTIL_NETWORK_FAIL_CREATESOCKET;
	}

	bzero((char *)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	memcpy((char *)&server_addr.sin_addr.s_addr,
		   resolvedaddr->Address,
		   resolvedaddr->Length);
	server_addr.sin_port = htons(port);

	while(true)
	{
		sockres = connect(fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
		if( sockres < 0)
		{
			write_log("Failed to connect to remove socket server "
					  "(errno %d), fd %d",
					  errno,
					  fd);
			if (errno == EINTR)
			{
				continue;
			}
			else
			{
				write_log("Close fd %d at once due to not recoverable connection"
						  "error detected.",
						  fd);
				closeConnectionRemote(&fd);
				return UTIL_NETWORK_FAIL_CONNECT;
			}
		}
		break;
	}

	*clientfd = fd;
	return FUNC_RETURN_OK;
}

/*
 * Close one existing connection, always return FUNC_RETURN_OK now.
 */
void closeConnectionRemote(int *clientfd)
{
	Assert(clientfd);
	if (*clientfd == -1)
	{
		return;
	}
	int ret = close(*clientfd);
	if (ret < 0)
	{
		write_log("Failed to close fd %d (errno %d)", *clientfd, errno);
	}
	*clientfd = -1;
}

char *format_time_microsec(uint64_t microtime)
{
	static char result[64];
	char timepart[32];
	time_t sec = microtime/1000000;
	struct tm *timeinfo = localtime(&sec);
	strftime(timepart, sizeof(timepart), "%F-%T", timeinfo);
	sprintf(result, "%s." UINT64_FORMAT, timepart, microtime - sec * 1000000);
	return result;
}

int readPipe(int fd, void *buff, int buffsize)
{
	int   s   = 0;
	int	  res = 0;
retry:
	res = piperead(fd, (char *)buff + s, buffsize - s);
	if ( res > 0 ) {
		s += res;
		if ( buffsize - s > 0 ) {
			goto retry;
		}
		return s;
	}
	else if ( res == 0 ) {
		return s;
	}
	else if ( res == -1 && (errno == EAGAIN || errno == EINTR)) {
		goto retry;
	}

	write_log("readPipe got read() error , fd %d, (errno %d)", fd, errno);

	return -1;
}

int writePipe(int fd, void *buff, int buffsize)
{
	int   s   = 0;
	int	  res = 0;
retry:
	res = pipewrite(fd, (char *)buff + s, buffsize - s);
	if ( res >= 0 ) {
		s += res;
		if ( buffsize - s > 0 ) {
			goto retry;
		}
		return s;
	}
	else if ( res == -1 && (errno == EAGAIN || errno == EINTR)) {
		goto retry;
	}

	write_log("writePipe got write() error , fd %d, (errno %d)", fd, errno);

	return -1;
}

/*
 * This function check buffered resolved host address by hostname string. If the
 * hostname is new to current process, gethostbyname() is called. If the hostname
 * is not resolved successfully, NULL is returned.
 */
AddressString getAddressStringByHostName(const char *hostname)
{
	AddressString res = NULL;

	/* Check HASHTABLE to see if this hostname has been successfully resolved. */
	SimpString key;
	setSimpleStringRef(&key, (char *)hostname, strlen(hostname));

	PAIR pair = getHASHTABLENode(&ResolvedHostnames, (void *)&key);
	if ( pair != NULL )
	{
		/* Return buffered host address content. */
		res = pair->Value;
		return res;
	}

	/* Resolve hostname and build up the object buffered in HASHTABLE. */
	struct hostent *server = gethostbyname(hostname);
	if ( server == NULL )
	{
		write_log("Failed to resolve hostname %s. (herrno %d)", hostname, h_errno);
		return NULL;
	}

	res = rm_palloc0(PCONTEXT,
					 offsetof(AddressStringData, Address) + server->h_length + 1);
	memcpy(res->Address, server->h_addr, server->h_length);
	res->Length = server->h_length;
	setHASHTABLENode(&ResolvedHostnames, (void *)&key, (void *)res, false);
	return res;
}

ConnAddressString createConnAddressString(AddressString address, uint16_t port)
{
	ConnAddressString res = rm_palloc0(PCONTEXT,
									   EXPSIZEOFCONNADDRSTRING(address));
	res->Port = port;
	res->Reserved = 0;
	res->Address.Length = address->Length;
	memcpy(res->Address.Address, address->Address, address->Length);
	return res;
}

void freeConnAddressString(ConnAddressString connaddr)
{
	rm_pfree(PCONTEXT, connaddr);
}

int fetchAliveSocketConnection(const char 	 *hostname,
							   AddressString  address,
							   uint16_t 	  port)
{
	ConnAddressString connaddr = createConnAddressString(address, port);
	SimpArray key;
	setSimpleArrayRef(&key, (char *)connaddr, SIZEOFCONNADDRSTRING(connaddr));
	PAIR pair = getHASHTABLENode(&ActiveConnections, (void *)&key);
	if ( pair == NULL )
	{
		freeConnAddressString(connaddr);
		return -1;
	}

	List *list = (List *)pair->Value;
	Assert(list != NULL);

	int res = lfirst_int(list_head(list));
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT);
	list = list_delete_first(list);
	MEMORY_CONTEXT_SWITCH_BACK

	if ( list == NULL )
	{
		/*
		 * If the last buffered connection for this address and port, remove
		 * the hash table node.
		 */
		removeHASHTABLENode(&ActiveConnections, (void *)&key);
	}

	freeConnAddressString(connaddr);
	elog(DEBUG3, "Fetched FD %d for %s:%d.", res, hostname, port);
	return res;
}

void returnAliveConnectionRemoteByHostname(int 		  *clientfd,
										   const char *hostname,
										   uint16_t port)
{
	/* Resolve hostname by checking hash table. */
	AddressString addrstr = getAddressStringByHostName(hostname);
	if ( addrstr == NULL )
	{
		closeConnectionRemote(clientfd);
	}
	else
	{
		returnAliveConnectionRemote(clientfd, hostname, addrstr, port);
	}

}

void returnAliveConnectionRemote(int 			*clientfd,
								 const char 	*hostname,
								 AddressString   addrstr,
								 uint16_t 		 port)
{

	/* In case no need to buffer connection, we close the connection directly. */
	if ( !rm_enable_connpool )
	{
		closeConnectionRemote(clientfd);
		return;
	}

	/* Try to get node. */
	ConnAddressString connaddr = createConnAddressString(addrstr, port);
	SimpArray key;
	setSimpleArrayRef(&key, (char *)connaddr, SIZEOFCONNADDRSTRING(connaddr));
	PAIR pair = getHASHTABLENode(&ActiveConnections, (void *)&key);

	List *list = NULL;
	if ( pair == NULL )
	{
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		list = list_make1_int(*clientfd);
		MEMORY_CONTEXT_SWITCH_BACK
		setHASHTABLENode(&ActiveConnections, (void *)&key, (void *)list, false);
		elog(DEBUG3, "Buffered FD %d for %s:%d.", *clientfd, hostname, port);
	}
	else
	{
		list = (List *)(pair->Value);
		if ( list_length(list) >= rm_connpool_sameaddr_buffersize )
		{
			elog(DEBUG3, "Drop FD %d because too many FDs buffered already for "
						 "the same address and port.",
						 *clientfd);
			closeConnectionRemote(clientfd);
		}
		else
		{
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			list = lappend_int(list, *clientfd);
			MEMORY_CONTEXT_SWITCH_BACK
			elog(DEBUG3, "Buffered FD %d for %s:%d.", *clientfd, hostname, port);
		}
		pair->Value = (void *)list;
	}

	*clientfd = -1;
	freeConnAddressString(connaddr);
}

void initializeSocketConnectionPool(void)
{
	/* Initialize the hash table for buffering resolved hosts. */
    initializeHASHTABLE(&ResolvedHostnames,
    					PCONTEXT,
						HASHTABLE_SLOT_VOLUME_DEFAULT,
						HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
						HASHTABLE_KEYTYPE_SIMPSTR,
						NULL);

    /* Initialize the hash table for buffering alive socket connections. */
    initializeHASHTABLE(&ActiveConnections,
        				PCONTEXT,
    					HASHTABLE_SLOT_VOLUME_DEFAULT,
    					HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
						HASHTABLE_KEYTYPE_CHARARRAY,
    					NULL);

    on_proc_exit(cleanupSocketConnectionPool, 0);
}

static void cleanupSocketConnectionPool(int code, Datum arg)
{
	/* Free alive connections. */
	List 	 *connlist 	= NULL;
	ListCell *cell 		= NULL;
	getAllPAIRRefIntoList(&ActiveConnections, &connlist);
	foreach(cell, connlist)
	{
		PAIR pair = (PAIR)lfirst(cell);
		List *aliveconns = (List *)(pair->Value);

		while( aliveconns != NULL )
		{
			int fd = lfirst_int(list_head(aliveconns));
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			aliveconns = list_delete_first(aliveconns);
			MEMORY_CONTEXT_SWITCH_BACK
			closeConnectionRemote(&fd);
		}
		pair->Value = NULL;
	}

	freePAIRRefList(&ActiveConnections, &connlist);
	cleanHASHTABLE(&ActiveConnections);

	/* Free buffered resolved hosts. */
	List 	 *addrlist	= NULL;
	getAllPAIRRefIntoList(&ResolvedHostnames, &addrlist);
	foreach(cell, addrlist)
	{
		AddressString addrstr = (AddressString)(((PAIR)lfirst(cell))->Value);
		rm_pfree(PCONTEXT, addrstr);
	}

	freePAIRRefList(&ResolvedHostnames, &addrlist);
	cleanHASHTABLE(&ResolvedHostnames);
}

int setConnectionLongTermNoDelay(int fd)
{
	int on;
#ifdef	TCP_NODELAY
	on = 1;
	if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *) &on, sizeof(on)) < 0)
	{
		elog(WARNING, "setsockopt(TCP_NODELAY) failed: %m");
		return SYSTEM_CALL_ERROR;
	}
#endif
	on = 1;
	if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char *) &on, sizeof(on)) < 0)
	{
		elog(WARNING, "setsockopt(SO_KEEPALIVE) failed: %m");
		return SYSTEM_CALL_ERROR;
	}
	return FUNC_RETURN_OK;
}
