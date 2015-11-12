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

int hostname_to_ipaddressstr(const char *hostname, char *ip )
{
	struct hostent *host;
	struct in_addr **address_list;

	host = gethostbyname(hostname);
	if ( host == NULL ) {
		return -1; /* Can not get host address from the name. */
	}

	address_list = (struct in_addr **)(host->h_addr_list);

	if ( address_list[0] == NULL )
		return -1; /* No usable address. */
	strcpy(ip, inet_ntoa(*address_list[0]));

	return 0;

}

int ipaddressstr_to_4bytes(const char *ip, uint8_t nums[4])
{
	int val[4];
	sscanf(ip, "%d.%d.%d.%d", val, val+1, val+2, val+3);
	nums[0] = val[0] & 0xFF;
	nums[1] = val[1] & 0xFF;
	nums[2] = val[2] & 0xFF;
	nums[3] = val[3] & 0xFF;

	return 0;
}

int ipaddress4bytes_to_str(uint8_t nums[4], char *ip)
{
	sprintf(ip, "%d.%d.%d.%d", nums[0], nums[1], nums[2], nums[3]);
	return 0;
}

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
			write_log("Fail to call gethostbyname() to get host %s, %s, herrno %d",
					  hostname,
					  hstrerror(h_errno),
					  h_errno);
			break;
		}
		pg_usleep(NETWORK_RETRY_SLEEP_US);
	}

	if ( hent == NULL )
	{
		write_log("WARNING. Fail to resolve host %s.", hostname);
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

int getLocalHostAllIPAddresses(DQueue addresslist)
{
	Assert(false);
	return FUNC_RETURN_OK;
}

int getLocalHostAllIPAddressesAsStrings(DQueue addresslist)
{
	int		 res	   = FUNC_RETURN_OK;
	struct ifaddrs *ifaddr, *ifa;
	int family, s, n;
	char host[NI_MAXHOST];

	if ( getifaddrs(&ifaddr) == -1 )
	{
		elog(ERROR, "Fail to get interface addresses when calling getifaddrs(), "
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

			elog(DEBUG3, "Resource manager discovered local host IPv4 address %s",
						 ((AddressString)(newaddr->Address))->Address);
		}
	}

	freeifaddrs(ifaddr);

	return res;
}

HostAddress createHostAddressAsStringFromIPV4Address(MCTYPE context, uint32_t addr)
{
	static char 	tmpaddrstr[16];
	static uint32_t tmpaddrlen = 0;
	HostAddress 	result = NULL;
	int				resultsize = 0;

	result = rm_palloc0(context, sizeof(HostAddressData));
	result->Attribute.Offset  = 0;
	result->Attribute.Mark   |= HOST_ADDRESS_CONTENT_STRING;

	ipaddress4bytes_to_str((uint8_t *)&addr, tmpaddrstr);
	tmpaddrlen = strlen(tmpaddrstr);
	resultsize = __SIZE_ALIGN64(offsetof(AddressStringData, Address) +
								tmpaddrlen + 1 );
	AddressString straddr = rm_palloc0(context, resultsize);

	straddr->Length = tmpaddrlen;
	strcpy(straddr->Address, tmpaddrstr);
	result->Address = (char *)straddr;
	result->AddressSize = resultsize;

	return result;
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
	  elog(WARNING, "setConnectionNonBlocked() fcntl GETFL failed, fd %d (errno %d)",
			  		fd,
					errno);
	  return SYSTEM_CALL_ERROR;
	}
	flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	if (flags == -1)
	{
	  elog(WARNING, "setConnectionNonBlocked() fcntl SETFL failed, fd %d (errno %d)",
			  	    fd,
					errno);
	  return SYSTEM_CALL_ERROR;
	}

	return FUNC_RETURN_OK;
}

/*
 * A wrapper for getting one unix domain socket connection to server.
 *
 * sockpath[in]			The domain socket file name.
 * port[in] 			The port number.
 * clientfd[out]		The fd of connection.
 *
 * Return:
 * FUNC_RETURN_OK					Succeed.
 * UTIL_NETWORK_FAIL_CREATESOCKET. 	Fail to call socket().
 * UTIL_NETWORK_FAIL_BIND. 			Fail to call bind().
 * UTIL_NETWORK_FAIL_CONNECT. 		Fail to call connect().
 **/
int  connectToServerDomain(const char 	*sockpath,
						   uint16_t 	 port,
						   int 			*clientfd,
						   int			 fileidx,
						   char			*filename)
{
	struct sockaddr_un  sockaddr;
	int					fd;
	int					len;
	int					sockres;

	*clientfd = -1;
	filename[0] = '\0';

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if ( fd < 0 ) {
	  write_log("connectToServerDomain open socket failed (errno %d)", errno);
		return UTIL_NETWORK_FAIL_CREATESOCKET;
	}

	memset( &sockaddr, 0, sizeof(struct sockaddr_un) );
	sockaddr.sun_family = AF_UNIX;
	sprintf(sockaddr.sun_path, "%s.%d.%lu.%d",
			sockpath,
			getpid(),
			(unsigned long)pthread_self(),
			fileidx);
	len = offsetof(struct sockaddr_un, sun_path) + strlen(sockaddr.sun_path);
	unlink(sockaddr.sun_path);
	strcpy(filename, sockaddr.sun_path);

	sockres = bind(fd, (struct sockaddr *)&sockaddr, len);
	if ( sockres < 0 ) {
	  write_log("connectToServerDomain bind socket failed %s, fd %d (errno %d)", filename, fd, errno);
	  closeConnectionDomain(&fd, filename);
	  return UTIL_NETWORK_FAIL_BIND;
	}

	memset( &sockaddr, 0, sizeof(struct sockaddr_un) );
	sockaddr.sun_family = AF_UNIX;
	sprintf(sockaddr.sun_path, "%s", sockpath);
	len = offsetof(struct sockaddr_un, sun_path) + strlen(sockaddr.sun_path);

	for ( int i = 0 ; i < DRM_SOCKET_CONN_RETRY ; ++i ) {
		sockres = connect(fd, (struct sockaddr *)&sockaddr, len);
		if ( sockres < 0 ) {
		  write_log("connectToServerDomain connect failed, fd %d (errno %d)", fd, errno);
		  pg_usleep(1000000); /* Sleep 2 seconds and retry. */
		}
		else {
		  break;
		}
	}

	if ( sockres < 0 ) {
	  write_log("connectToServerDomain connect failed after retry, fd %d (errno %d)", fd, errno);
	  closeConnectionDomain(&fd, filename);
	  return UTIL_NETWORK_FAIL_CONNECT;
	}

	*clientfd = fd;
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
	int					fd		= 0;
	int 		    	sockres = 0;
	struct sockaddr_in 	server_addr;
	struct hostent 	   *server  = gethostbyname(address);

	*clientfd = -1;

	if ( server == NULL ) {
		return UTIL_NETWORK_FAIL_GETHOST;
	}

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if ( fd < 0 ) {
	  write_log("connectToServerRemote open socket failed (errno %d)", errno);
		return UTIL_NETWORK_FAIL_CREATESOCKET;
	}

	bzero((char *)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	bcopy((char *)server->h_addr,
		  (char *)&server_addr.sin_addr.s_addr,
		  server->h_length);
	server_addr.sin_port = htons(port);

	while(true)
	{
	  sockres = connect(fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
	  if( sockres < 0)
	  {
	    if (errno == EINTR)
	    {
	      continue;
	    }
	    else
	    {
	      write_log("connectToServerRemote connect failed, fd %d (errno %d)", fd, errno);
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
		write_log("ERROR closeConnectionRemote() close FD %d failed, (errno %d)",
				  *clientfd,
				  errno);
	}
	*clientfd = -1;
}

void closeConnectionDomain(int *clientfd, char *filename)
{
	Assert(clientfd);
	if (*clientfd == -1)
	{
		return;
	}
	else
	{
		int ret = close(*clientfd);
		if (ret < 0)
		{
			write_log("closeConnectionDomain close fd failed, fd %d (errno %d)", *clientfd, errno);
		}
	}

	if ( filename != NULL && filename[0] != '\0' )
	{
		unlink(filename);
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
