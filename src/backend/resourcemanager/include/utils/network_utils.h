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

#ifndef RESOURCE_MANANGER_NETWORK_UTILITIES_H
#define RESOURCE_MANANGER_NETWORK_UTILITIES_H

#include <netdb.h>
#include <arpa/inet.h>

#include "resourcemanager/envswitch.h"
#include "resourcemanager/utils/simplestring.h"
#include "resourcemanager/utils/linkedlist.h"

/******************************************************************************
 * miscs
 *****************************************************************************/
#define SOCKADDR(addr) inet_ntoa(((struct sockaddr_in *)(addr))->sin_addr)
#define SOCKPORT(addr) ((struct sockaddr_in *)(addr))->sin_port

#define IPV4_DOT_ADDR_LO "127.0.0.1"

#define NETWORK_RETRY_SLEEP_US 1000000
#define NETWORK_RETRY_TIMES    10

uint64_t gettime_microsec(void);

struct AddressIPV4Data {
	uint32_t		Address;
	uint32_t		Reserved;
};

struct AddressIPV6Data {
	uint32_t		Address[4];
};

struct AddressStringData {
	uint32_t		Length;
	char			Address[1];
};

struct AddressAttributeData {
	uint16_t		Offset;
	uint16_t		Mark;
};

typedef struct AddressIPV4Data    	 AddressIPV4Data;
typedef struct AddressIPV4Data   	*AddressIPV4;
typedef struct AddressIPV6Data    	 AddressIPV6Data;
typedef struct AddressIPV6Data   	*AddressIPV6;
typedef struct AddressStringData  	 AddressStringData;
typedef struct AddressStringData 	*AddressString;
typedef struct AddressAttributeData  AddressAttributeData;
typedef struct AddressAttributeData *AddressAttribute;

#define ADDRESS_STRING_ALL_SIZE(addr) 										   \
		(offsetof(AddressStringData, Address) + (addr)->Length + 1)

struct HostAddressData {
	AddressAttributeData	Attribute;
	int32_t					AddressSize;
	char				   *Address;
};

typedef struct HostAddressData  	 HostAddressData;
typedef struct HostAddressData 		*HostAddress;


/* The bit value of AddressOffsetData::Mark. */
#define HOST_ADDRESS_TYPE_IPV4			0X0
#define HOST_ADDRESS_TYPE_IPV6			0X1
#define HOST_ADDRESS_CONTENT_STRING		0X2

HostAddress createHostAddressAsStringFromIPV4AddressStr(MCTYPE context, const char *addr);
void freeHostAddress(MCTYPE context, HostAddress addr);
/* In case we have only host name of a remote machine, we need this one to find
 * usable ipv4 address to communicate with. */
int getHostIPV4AddressesByHostNameAsString(MCTYPE 	 		context,
										   const char 	   *hostname,
										   SimpStringPtr 	ohostname,
										   List	 		  **addresses);
void freeHostIPV4AddressesAsString(MCTYPE context, List **addresses);
/* APIs for getting machine local network configurations. Including ip addresses,
 * host name, physical core and memory information. */
int getLocalHostName(SimpStringPtr hostname);

int getLocalHostAllIPAddressesAsStrings(DQueue addresslist);

bool AddressStringComp(AddressString addr1, AddressString addr2);

int sendWithRetry(int fd, char *buff, int size, bool checktrans);
int recvWithRetry(int fd, char *buff, int size, bool checktrans);
int setConnectionNonBlocked(int fd);

#define DRM_SOCKET_CONN_RETRY 5
int  connectToServerRemote(const char *address,uint16_t port,int *clientfd);
int setConnectionLongTermNoDelay(int fd);
void closeConnectionRemote(int *clientfd);
void returnAliveConnectionRemote(int 			*clientfd,
								 const char 	*hostname,
								 AddressString   addrstr,
								 uint16_t 		 port);
void returnAliveConnectionRemoteByHostname(int 		  *clientfd,
										   const char *hostname,
										   uint16_t    port);
char *format_time_microsec(uint64_t microtime);

int readPipe(int fd, void *buff, int buffsize);
int writePipe(int fd, void *buff, int buffsize);

struct ConnAddressStringData {
	uint16_t				Port;
	uint16_t				Reserved;
	AddressStringData		Address;
};
typedef struct ConnAddressStringData	 ConnAddressStringData;
typedef struct ConnAddressStringData	*ConnAddressString;

#define SIZEOFCONNADDRSTRING(connaddr) offsetof(ConnAddressStringData,Address)+\
									   offsetof(AddressStringData,Address)+\
									   (connaddr)->Address.Length+1

#define EXPSIZEOFCONNADDRSTRING(addrstr) offsetof(ConnAddressStringData,Address)+\
		   	   	   	   	   	   	   	     offsetof(AddressStringData,Address)+\
										 (addrstr)->Length+1

ConnAddressString createConnAddressString(AddressString address, uint16_t port);
void freeConnAddressString(ConnAddressString connaddr);

void initializeSocketConnectionPool(void);
AddressString getAddressStringByHostName(const char *hostname);

int fetchAliveSocketConnection(const char 	 *hostname,
							   AddressString  address,
							   uint16_t 	  port);
#endif /* RESOURCE_MANANGER_NETWORK_UTILITIES_H */
