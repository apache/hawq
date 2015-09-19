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
int hostname_to_ipaddressstr(const char *hostname, char *ip );
//int hostname_to_ipaddress4bytes(const char *hostname, uint8_t nums[4] );
int ipaddressstr_to_4bytes(const char *ip, uint8_t nums[4]);
int ipaddress4bytes_to_str(uint8_t nums[4], char *ip);

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

HostAddress createHostAddressAsStringFromIPV4Address(MCTYPE context, uint32_t addr);
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
int getLocalHostAllIPAddresses(DQueue addresslist);

bool AddressStringComp(AddressString addr1, AddressString addr2);

int sendWithRetry(int fd, char *buff, int size, bool checktrans);
int recvWithRetry(int fd, char *buff, int size, bool checktrans);
int setConnectionNonBlocked(int fd);

#define DRM_SOCKET_CONN_RETRY 5
int  connectToServerDomain(const char 	*sockpath,
						   uint16_t 	 port,
						   int 			*clientfd,
						   int			 fileidx,
						   char			*filename);
int  connectToServerRemote(const char *address,uint16_t port,int *clientfd);
void closeConnectionRemote(int *clientfd);
void closeConnectionDomain(int *clientfd, char *filename);

char *format_time_microsec(uint64_t microtime);

int readPipe(int fd, void *buff, int buffsize);
int writePipe(int fd, void *buff, int buffsize);

#endif /* RESOURCE_MANANGER_NETWORK_UTILITIES_H */
