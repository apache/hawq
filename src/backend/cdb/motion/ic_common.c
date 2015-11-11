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

/*-------------------------------------------------------------------------
 * ic_common.c
 *	   Interconnect code shared between UDP, and TCP IPC Layers.
 *
 * Reviewers: jzhang, tkordas
 *-------------------------------------------------------------------------
 */

#ifdef WIN32
/*
 * Need this to get WSAPoll (poll). And it
 * has to be set before any header from the Win32 API is loaded.
 */
#undef _WIN32_WINNT
#define _WIN32_WINNT 0x0600
#endif

#include "postgres.h"

#include <pthread.h>

#include "nodes/execnodes.h"            /* Slice, SliceTable */
#include "nodes/pg_list.h"
#include "nodes/print.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "miscadmin.h"
#include "libpq/libpq-be.h"
#include "libpq/ip.h"
#include "utils/builtins.h"
#include "utils/debugbreak.h"

#include "cdb/cdbselect.h"
#include "cdb/tupchunklist.h"
#include "cdb/ml_ipc.h"
#include "cdb/cdbvars.h"

#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "port.h"

#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0600
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#define SHUT_RDWR SD_BOTH
#define SHUT_RD SD_RECEIVE
#define SHUT_WR SD_SEND

/* If we have old platform sdk headers, WSAPoll() might not be there */
#ifndef POLLIN
/* Event flag definitions for WSAPoll(). */

#define POLLRDNORM  0x0100
#define POLLRDBAND  0x0200
#define POLLIN      (POLLRDNORM | POLLRDBAND)
#define POLLPRI     0x0400

#define POLLWRNORM  0x0010
#define POLLOUT     (POLLWRNORM)
#define POLLWRBAND  0x0020

#define POLLERR     0x0001
#define POLLHUP     0x0002
#define POLLNVAL    0x0004

typedef struct pollfd {

    SOCKET  fd;
    SHORT   events;
    SHORT   revents;

} WSAPOLLFD, *PWSAPOLLFD, FAR *LPWSAPOLLFD;
__control_entrypoint(DllExport)
WINSOCK_API_LINKAGE
int
WSAAPI
WSAPoll(
    IN OUT LPWSAPOLLFD fdArray,
    IN ULONG fds,
    IN INT timeout
    );
#endif

#define poll WSAPoll

/*
 * Postgres normally uses it's own custom select implementation
 * on Windows, but they haven't implemented execeptfds, which
 * we use here.  So, undef this to use the normal Winsock version
 * for now
 */
#undef select
#endif

/*
  #define AMS_VERBOSE_LOGGING
*/

/*=========================================================================
 * STRUCTS
 */


/*=========================================================================
 * GLOBAL STATE VARIABLES
 */

/* Socket file descriptor for the listener. */
int		TCP_listenerFd;
int		UDP_listenerFd;

/* Socket file descriptor for the sequence server. */
int		savedSeqServerFd = -1;
char	*savedSeqServerHost = NULL;
uint16	savedSeqServerPort = 0;

/*
 * Outgoing port assignment
 *
 * To reserve a port number for outgoing connections, we open a dummy
 * listening socket.  Nobody connects to this socket.
 */
int		portReservationFd = -1;
int		outgoingPort = 0;

/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */

static void setupSeqServerConnection(char *hostname, uint16 port);

#ifdef AMS_VERBOSE_LOGGING
static void	dumpEntryConnections(int elevel, ChunkTransportStateEntry *pEntry);
static void	print_connection(ChunkTransportState *transportStates, int fd, const char *msg);
#endif

static void
logChunkParseDetails(MotionConn *conn)
{
	struct icpkthdr *pkt;

	Assert(conn != NULL);
	Assert(conn->pBuff != NULL);

	pkt = (struct icpkthdr *)conn->pBuff;

	elog(LOG, "Interconnect parse details: pkt->len %d pkt->seq %d pkt->flags 0x%x conn->active %d conn->stopRequest %d pkt->icId %d my_icId %d",
		 pkt->len, pkt->seq, pkt->flags, conn->stillActive, conn->stopRequested, pkt->icId, gp_interconnect_id);

	elog(LOG, "Interconnect parse details continued: peer: srcpid %d dstpid %d recvslice %d sendslice %d srccontent %d dstcontent %d",
		 pkt->srcPid, pkt->dstPid, pkt->recvSliceIndex, pkt->sendSliceIndex, pkt->srcContentId, pkt->dstContentId);
}

TupleChunkListItem
RecvTupleChunk(MotionConn *conn, bool inTeardown)
{
	TupleChunkListItem tcItem;
	TupleChunkListItem firstTcItem = NULL;
	TupleChunkListItem lastTcItem = NULL;
	uint32		tcSize;
	int			bytesProcessed = 0;

	if (Gp_interconnect_type == INTERCONNECT_TYPE_TCP)
	{
		/* read the packet in from the network. */
		readPacket(conn, inTeardown);

		/* go through and form us some TupleChunks. */
		bytesProcessed = PACKET_HEADER_SIZE;
	}
	else
	{
		/* go through and form us some TupleChunks. */
		bytesProcessed = sizeof(struct icpkthdr);
	}

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "recvtuple chunk recv bytes %d msgsize %d conn->pBuff %p conn->msgPos: %p",
		 conn->recvBytes, conn->msgSize, conn->pBuff, conn->msgPos);
#endif

	while (bytesProcessed != conn->msgSize)
	{
		if (conn->msgSize - bytesProcessed < TUPLE_CHUNK_HEADER_SIZE)
		{
			logChunkParseDetails(conn);

			ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
							errmsg("Interconnect error parsing message: insufficient data received."),
							errdetail("conn->msgSize %d bytesProcessed %d < chunk-header %d",
									  conn->msgSize, bytesProcessed, TUPLE_CHUNK_HEADER_SIZE)));
		}

		tcSize = TUPLE_CHUNK_HEADER_SIZE + (*(uint16 *) (conn->msgPos + bytesProcessed));

		/* sanity check */
		if (tcSize > Gp_max_packet_size)
		{
			/* see MPP-720: it is possible that our message got messed
			 * up by a cancellation ? */
			ML_CHECK_FOR_INTERRUPTS(inTeardown);

			/*
			 * MPP-4010: add some extra debugging.
			 */
			if (lastTcItem != NULL)
				elog(LOG, "Interconnect error parsing message: last item length %d inplace %p", lastTcItem->chunk_length, lastTcItem->inplace);
			else
				elog(LOG, "Interconnect error parsing message: no last item");

			logChunkParseDetails(conn);

			ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
							errmsg("Interconnect error parsing message"),
							errdetail("tcSize %d > max %d header %d processed %d/%d from %p",
									  tcSize, Gp_max_packet_size,
									  TUPLE_CHUNK_HEADER_SIZE, bytesProcessed, conn->msgSize, conn->msgPos)));
		}


		/* we only check for interrupts here when we don't have a guaranteed full-message */
		if (Gp_interconnect_type == INTERCONNECT_TYPE_TCP)
		{
			if (tcSize >= conn->msgSize)
			{
				/* see MPP-720: it is possible that our message got
				 * messed up by a cancellation ? */
				ML_CHECK_FOR_INTERRUPTS(inTeardown);

				logChunkParseDetails(conn);

				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								errmsg("Interconnect error parsing message"),
								errdetail("tcSize %d >= conn->msgSize %d", tcSize, conn->msgSize)));
			}
		}
		Assert(tcSize < conn->msgSize);

		/*
		 * We store the data inplace, and handle any necessary copying later
		 * on
		 */
		tcItem = (TupleChunkListItem) palloc0(sizeof(TupleChunkListItemData));

		tcItem->chunk_length = tcSize;
		tcItem->inplace = (char *) (conn->msgPos + bytesProcessed);

		bytesProcessed += TYPEALIGN(TUPLE_CHUNK_ALIGN,tcSize);

		if (firstTcItem == NULL)
		{
			firstTcItem = tcItem;
			lastTcItem = tcItem;
		}
		else
		{
			lastTcItem->p_next = tcItem;
			lastTcItem = tcItem;
		}
	}

	conn->recvBytes -= conn->msgSize;
	if (conn->recvBytes != 0)
	{
#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "residual message %d bytes", conn->recvBytes);
#endif
		conn->msgPos += conn->msgSize;
	}

	conn->msgSize = 0;

	return firstTcItem;
}

/*=========================================================================
 * VISIBLE FUNCTIONS
 */

/* See ml_ipc.h */
void
InitMotionLayerIPC(void)
{
	uint16 tcp_listener = 0;
	uint16 udp_listener = 0;

	/*activated = false;*/
	savedSeqServerFd = -1;

	InitMotionTCP(&TCP_listenerFd, &tcp_listener);
	InitMotionUDP(&UDP_listenerFd, &udp_listener);

	Gp_listener_port = (udp_listener<<16) | tcp_listener;

	elog(DEBUG1, "Interconnect listening on tcp port %d udp port %d (0x%x)", tcp_listener, udp_listener, Gp_listener_port);
}

/* See ml_ipc.h */
void
CleanUpMotionLayerIPC(void)
{
	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
    	elog(DEBUG3, "Cleaning Up Motion Layer IPC...");

	CleanupMotionTCP();
	CleanupMotionUDP();

	/* close down the Interconnect listener socket. */
    if (TCP_listenerFd >= 0)
        closesocket(TCP_listenerFd);

	if (UDP_listenerFd >= 0)
		closesocket(UDP_listenerFd);

    if (portReservationFd >= 0)
        closesocket(portReservationFd);

	/* be safe and reset global state variables. */
	Gp_listener_port = 0;
	TCP_listenerFd = -1;
	UDP_listenerFd = -1;
    portReservationFd = -1;
}

/* See ml_ipc.h */
bool
SendTupleChunkToAMS(MotionLayerState *mlStates,
					ChunkTransportState *transportStates,
					int16 motNodeID,
					int16 targetRoute,
					TupleChunkListItem tcItem)
{
	int			i,
		recount = 0;
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn;
	TupleChunkListItem currItem;

	if (!transportStates)
		elog(FATAL, "SendTupleChunkToAMS: no transport-states.");
	if (!transportStates->activated)
		elog(FATAL, "SendTupleChunkToAMS: transport states inactive");

	/* check em' */
	ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG3, "sendtuplechunktoams: calling get_transport_state"
		 "w/transportStates %p transportState->size %d motnodeid %d route %d",
		 transportStates, transportStates->size, motNodeID, targetRoute);
#endif

	getChunkTransportState(transportStates, motNodeID, &pEntry);

	/*
	 * tcItem can actually be a chain of tcItems.  we need to send out all of
	 * them.
	 */
	currItem = tcItem;

	for (currItem = tcItem; currItem != NULL; currItem = currItem->p_next)
	{
#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "SendTupleChunkToAMS: chunk length %d", currItem->chunk_length);
#endif

		if (targetRoute == BROADCAST_SEGIDX)
		{
			doBroadcast(mlStates, transportStates, pEntry, currItem, &recount);
		}
		else
		{
			/* handle pt-to-pt message. Primary */
			conn = pEntry->conns + targetRoute;
			/* only send to interested connections */
			if (conn->stillActive)
			{
				transportStates->SendChunk(mlStates, transportStates, pEntry, conn, currItem, motNodeID);
				if (!conn->stillActive)
					recount = 1;
			}
			/* in 4.0 logical mirror xmit eliminated. */
		}
	}

	if (recount == 0)
		return true;

	/* if we don't have any connections active, return false */
	for (i = 0; i < pEntry->numConns; i++)
	{
		conn = pEntry->conns + i;
		if (conn->stillActive)
			break;
	}

	/* if we found an active connection we're not done */
	return (i < pEntry->numConns);
}

/*
 * The fetches a direct pointer into our transmit buffers, along with
 * an indication as to how much data can be safely shoved into the
 * buffer (started at the pointed location).
 *
 * This works a lot like SendTupleChunkToAMS().
 */
void
getTransportDirectBuffer(ChunkTransportState *transportStates,
						 int16 motNodeID,
						 int16 targetRoute,
						 struct directTransportBuffer *b)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn;

	if (!transportStates)
	{
		elog(FATAL, "getTransportDirectBuffer: no transport states");
	}
	else if (!transportStates->activated)
	{
		elog(FATAL, "getTransportDirectBuffer: inactive transport states");
	}
	else if (targetRoute == BROADCAST_SEGIDX)
	{
		elog(FATAL, "getTransportDirectBuffer: can't direct-transport to broadcast");
	}

	Assert(b != NULL);

	do
	{
		getChunkTransportState(transportStates, motNodeID, &pEntry);

		/* handle pt-to-pt message. Primary */
		conn = pEntry->conns + targetRoute;
		/* only send to interested connections */
		if (!conn->stillActive)
		{
			break;
		}

		b->pri = conn->pBuff + conn->msgSize;
		b->prilen = Gp_max_packet_size - conn->msgSize;

		/* got buffer. */
		return;
	}
	while (0);

	/* buffer is missing ? */

	b->pri = NULL;
	b->prilen = 0;

	return;
}

/*
 * The fetches a direct pointer into our transmit buffers, along with
 * an indication as to how much data can be safely shoved into the
 * buffer (started at the pointed location).
 *
 * This works a lot like SendTupleChunkToAMS().
 */
void
putTransportDirectBuffer(ChunkTransportState *transportStates,
						 int16 motNodeID,
						 int16 targetRoute, int length)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn;

	if (!transportStates)
	{
		elog(FATAL, "putTransportDirectBuffer: no transport states");
	}
	else if (!transportStates->activated)
	{
		elog(FATAL, "putTransportDirectBuffer: inactive transport states");
	}
	else if (targetRoute == BROADCAST_SEGIDX)
	{
		elog(FATAL, "putTransportDirectBuffer: can't direct-transport to broadcast");
	}

	getChunkTransportState(transportStates, motNodeID, &pEntry);

	/* handle pt-to-pt message. Primary */
	conn = pEntry->conns + targetRoute;
	/* only send to interested connections */
	if (conn->stillActive)
	{
		conn->msgSize += length;
		conn->tupleCount++;
	}

	/* put buffer. */
	return;
}

/*
 * DeregisterReadInterest is called on receiving nodes when they
 * believe that they're done with the receiver
 */
void
DeregisterReadInterest(ChunkTransportState *transportStates,
                       int		motNodeID,
                       int		srcRoute,
                       const char		*reason)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn;

	if (!transportStates)
	{
		elog(FATAL, "DeregisterReadInterest: no transport states");
	}

	if (!transportStates->activated)
		return;

	getChunkTransportState(transportStates, motNodeID, &pEntry);
	conn = pEntry->conns + srcRoute;

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
    {
        elog(DEBUG3, "Interconnect finished receiving "
             "from seg%d slice%d %s pid=%d sockfd=%d; %s",
             conn->remoteContentId,
             pEntry->sendSlice->sliceIndex,
             conn->remoteHostAndPort,
             conn->cdbProc->pid,
             conn->sockfd,
             reason);
    }

	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDP)
	{
#ifdef AMS_VERBOSE_LOGGING
		elog(LOG, "deregisterReadInterest set stillactive = false for node %d route %d (%s)", motNodeID, srcRoute, reason);
#endif
		markUDPConnInactive(conn);
	}
	else
	{

		/*
		 * we also mark the connection as "done." The way synchronization works is
		 * strange. On QDs the "teardown" doesn't get called until all segments
		 * are finished, which means that we need some way for the QEs to know
		 * that Teardown should complete, otherwise we deadlock the entire query
		 * (QEs wait in their Teardown calls, while the QD waits for them to
		 * finish)
		 */
		shutdown(conn->sockfd, SHUT_WR);

		MPP_FD_CLR(conn->sockfd, &pEntry->readSet);
	}
	return;
}

/*
 * Returns the fd of the socket that connects to the seqserver.  This value
 * is -1 if it has not been setup.
 */
int
GetSeqServerFD(void)
{
	/*
	 * setup connection to seq server if needed. The interconnect is
	 * responsible for maintaining the connection although it actually doesn't
	 * use the socket directly.  sequence.c does to obtain sequence values
	 * from the seqserver.	TeardownInterconnect() is responsible for closing
	 * the socket.
	 *
	 */
	if (savedSeqServerHost == NULL)
		elog(ERROR, "Invalid Sequence Access. Sequence server info is invalid.");

	if (savedSeqServerFd == -1)
		setupSeqServerConnection(savedSeqServerHost, savedSeqServerPort);

	return savedSeqServerFd;
}

void
SetupSequenceServer(const char *host, int port)
{
	if (host != NULL)
	{
		/*
		 * See MPP-10162: certain PL/PGSQL functions may call us multiple
		 * times without an intervening Teardown.
		 */
		if (savedSeqServerHost != NULL)
		{
			free(savedSeqServerHost);
			savedSeqServerHost = NULL;
			savedSeqServerPort = 0;
		}

		/*
		 * Don't use MemoryContexts -- they make error handling
		 * difficult here.
		 */
		savedSeqServerHost = strdup(host);

		if (savedSeqServerHost == NULL)
		{
			elog(ERROR, "SetupSequenceServer: memory allocation failed.");
		}

		Assert(port != 0);
		savedSeqServerPort = port;
	}
}

void
TeardownSequenceServer(void)
{
	/*
	 * If we setup a connection to the seqserver then we need to disconnect
	 */
	if (savedSeqServerFd != -1)
	{
	    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		    elog(DEBUG3, "tearing down seqserver connection");

		shutdown(savedSeqServerFd, SHUT_RDWR);
		closesocket(savedSeqServerFd);
		savedSeqServerFd = -1;
	}

	if (savedSeqServerHost != NULL)
	{
		free(savedSeqServerHost);
		savedSeqServerHost = NULL;
		savedSeqServerPort = 0;
	}
}

static void
setupSeqServerConnection(char *seqServerHost, uint16 seqServerPort)
{
	int 		n;
	int 		ret;
	char 		portNumberStr[32];
	char 	   *service;
	struct addrinfo *addrs = NULL;
	struct addrinfo hint;
	/*
	 * We get the IP address (IPv4 or IPv6) of the sequence server,
	 * not it's name, so we can tell getaddrinfo to skip any attempt at
	 * name resolution.
	 */

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;		/* Allow for IPv4 or IPv6  */
#ifdef AI_NUMERICSERV
	hint.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;  /* Never do name resolution */
#else
	hint.ai_flags = AI_NUMERICHOST;  /* Never do name resolution */
#endif


	snprintf(portNumberStr, sizeof(portNumberStr), "%d", seqServerPort);
	service = portNumberStr;

	ret = pg_getaddrinfo_all(seqServerHost, service, &hint, &addrs);
	if (ret || !addrs)
	{
		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);

		ereport(ERROR,
				(errmsg("could not translate host addr \"%s\", port \"%d\" to address: %s",
						seqServerHost, seqServerPort, gai_strerror(ret))));
		return;
	}

	if ((savedSeqServerFd = socket(addrs->ai_family, addrs->ai_socktype, addrs->ai_protocol)) < 0)
		elog(ERROR,"socket() call failed: %m");

	if ((n = connect(savedSeqServerFd, addrs->ai_addr, addrs->ai_addrlen)) < 0)
	{
		pg_freeaddrinfo_all(hint.ai_family, addrs);
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						errmsg("Interconnect Error: Could not connect to seqserver."),
		                errdetail("%m%s", "connect")));
	}

	pg_freeaddrinfo_all(hint.ai_family, addrs);

	/* make socket non-blocking BEFORE we connect. */
	if (!pg_set_noblock(savedSeqServerFd))
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						errmsg("Interconnect Error: Could not set seqserver socket"
						       "to non-blocking mode."),
			            errdetail("%m%s sockfd=%d", "fcntl", savedSeqServerFd)));
}

void
SetupInterconnect(EState *estate)
{
	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDP)
		SetupUDPInterconnect(estate);
	else if (Gp_interconnect_type == INTERCONNECT_TYPE_TCP)
		SetupTCPInterconnect(estate);
	else
	{
		if (Gp_interconnect_type != INTERCONNECT_TYPE_NIL)
		{
			elog(FATAL, "SetupUDPInterconnect: unknown interconnect-type");
		}

		if (estate->interconnect_context)
		{
			elog(FATAL, "SetupInterconnect: already initialized.");
		}

		if (!estate->es_sliceTable)
		{
			elog(FATAL, "SetupInterconnect: no slice table ?");
		}

		estate->interconnect_context = palloc0(sizeof(ChunkTransportState));

		/* initialize state variables */
		Assert(estate->interconnect_context->size == 0);
		estate->interconnect_context->size = CTS_INITIAL_SIZE;
		estate->interconnect_context->states = palloc0(CTS_INITIAL_SIZE * sizeof(ChunkTransportStateEntry));

		estate->interconnect_context->teardownActive = false;
		estate->interconnect_context->activated = false;
		estate->interconnect_context->incompleteConns = NIL;
		estate->interconnect_context->sliceTable = NULL;
		estate->interconnect_context->sliceId = -1;

		estate->interconnect_context->sliceTable = estate->es_sliceTable;

		estate->interconnect_context->sliceId = LocallyExecutingSliceIndex(estate);
	}

	return;
}


/*
 * Move this out to separate stack frame, so that we don't have to mark
 * tons of stuff volatile in TeardownInterconnect().
 */
void
forceEosToPeers(MotionLayerState       *mlStates,
			    ChunkTransportState    *transportStates,
			    int                     motNodeID)
{
	if (!transportStates)
	{
		elog(FATAL, "no transport-states.");
	}

	transportStates->teardownActive = true;

	transportStates->SendEos(mlStates, transportStates, motNodeID, get_eos_tuplechunklist());

	transportStates->teardownActive = false;
}

/* TeardownInterconnect() function is used to cleanup interconnect resources that
 * were allocated during SetupInterconnect().  This function should ALWAYS be
 * called after SetupInterconnect to avoid leaking resources (like sockets)
 * even if SetupInterconnect did not complete correctly.
 */
void
TeardownInterconnect(ChunkTransportState *transportStates,
					 MotionLayerState *mlStates,
					 bool forceEOS)
{
	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDP)
	{
		TeardownUDPInterconnect(transportStates, mlStates, forceEOS);
		return;
	}
	else if (Gp_interconnect_type == INTERCONNECT_TYPE_TCP)
	{
		TeardownTCPInterconnect(transportStates, mlStates, forceEOS);
		return;
	}
	else if (Gp_interconnect_type == INTERCONNECT_TYPE_NIL)
	{
		if (transportStates->states != NULL)
			pfree(transportStates->states);
		pfree(transportStates);

		return;
	}
}

/*=========================================================================
 * HELPER FUNCTIONS
 */


/* Function createChunkTransportState() is used to create a ChunkTransportState struct and
 * place it in the hashtab hashtable based on the motNodeID.
 *
 * PARAMETERS
 *
 *	 motNodeID - motion node ID for this ChunkTransportState.
 *
 *	 numPrimaryConns  - number of primary connections for this motion node.
 *               All are incoming if this is a receiving motion node.
 *               All are outgoing if this is a sending motion node.
 *
 * RETURNS
 *	 An empty and initialized ChunkTransportState struct for the given motion node. If
 *	 a ChuckTransportState struct is already registered for the motNodeID an ERROR is
 *	 thrown.
 */
ChunkTransportStateEntry *
createChunkTransportState(ChunkTransportState *transportStates,
						  Slice *sendSlice,
						  Slice *recvSlice,
						  int numPrimaryConns)
{
	ChunkTransportStateEntry *pEntry;
    int         motNodeID;
	int			i;

    Assert(recvSlice &&
           IsA(recvSlice, Slice) &&
           recvSlice->sliceIndex >= 0);
    Assert(sendSlice &&
           IsA(sendSlice, Slice) &&
           sendSlice->sliceIndex > 0);

    motNodeID = sendSlice->sliceIndex;
    if (motNodeID > transportStates->size)
	{
		/* increase size of our table */
		ChunkTransportStateEntry *newTable;

		newTable = repalloc(transportStates->states, motNodeID * sizeof(ChunkTransportStateEntry));
		transportStates->states = newTable;
		/* zero-out the new piece at the end */
		MemSet(&transportStates->states[transportStates->size], 0, (motNodeID - transportStates->size) * sizeof(ChunkTransportStateEntry));
		transportStates->size = motNodeID;
	}

	pEntry = &transportStates->states[motNodeID - 1];

	if (pEntry->valid)
	{
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						errmsg("Interconnect Error: A HTAB entry for motion node %d already exists.", motNodeID),
						errdetail("conns %p numConns %d first sock %d highreadsock %d", pEntry->conns, pEntry->numConns, pEntry->conns[0].sockfd, pEntry->highReadSock)));
	}

	pEntry->valid = true;

	pEntry->motNodeId = motNodeID;
    pEntry->numConns = numPrimaryConns;
	pEntry->numPrimaryConns = numPrimaryConns;
	pEntry->highReadSock = 0;
    pEntry->scanStart = 0;
    pEntry->sendSlice = sendSlice;
    pEntry->recvSlice = recvSlice;
    pEntry->outgoingPortRetryCount = 0;

	pEntry->conns = palloc0(pEntry->numConns * sizeof(pEntry->conns[0]));

	for (i = 0; i < pEntry->numConns; i++)
    {
        MotionConn *conn = &pEntry->conns[i];

        /* Initialize MotionConn entry. */
        conn->state = mcsNull;
        conn->sockfd = -1;
        conn->msgSize = 0;
        conn->tupleCount = 0;
        conn->stillActive = false;
        conn->stopRequested = false;
        conn->wakeup_ms = 0;
        conn->cdbProc = NULL;
    }

	MPP_FD_ZERO(&pEntry->readSet);

	return pEntry;
}

/* Function removeChunkTransportState() is used to remove a ChunkTransportState struct from
 * the hashtab hashtable.
 *
 * This should only be called after createChunkTransportState().
 *
 * PARAMETERS
 *
 *	 motNodeID - motion node ID to lookup the ChunkTransportState.
 *   pIncIdx - parent slice idx in child slice.  If not multiplexed, should be 1.
 *
 * RETURNS
 *	 The ChunkTransportState that was removed from the hashtab hashtable.
 */
ChunkTransportStateEntry *
removeChunkTransportState(ChunkTransportState *transportStates,
						  int16 motNodeID)
{
	ChunkTransportStateEntry *pEntry = NULL;

	if (motNodeID > transportStates->size)
	{
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						errmsg("Interconnect Error: Unexpected Motion Node Id: %d. During remove. (size %d)",
							   motNodeID, transportStates->size)));
	}
	else if (!transportStates->states[motNodeID - 1].valid)
	{
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						errmsg("Interconnect Error: Unexpected Motion Node Id: %d. During remove. State not valid",
							   motNodeID)));
	}
	else
	{
		transportStates->states[motNodeID - 1].valid = false;
		pEntry = &transportStates->states[motNodeID - 1];
	}

	return pEntry;
}

#ifdef AMS_VERBOSE_LOGGING
void
dumpEntryConnections(int elevel, ChunkTransportStateEntry *pEntry)
{
	int			i;
    MotionConn *conn;

    for (i = 0; i < pEntry->numConns; i++)
	{
		conn = &pEntry->conns[i];
        if (conn->sockfd == -1 &&
            conn->state == mcsNull)
            elog(elevel, "... motNodeId=%d conns[%d]:         not connected",
				 pEntry->motNodeId, i);
        else
            elog(elevel, "... motNodeId=%d conns[%d]:  "
				 "%s%d pid=%d sockfd=%d remote=%s local=%s",
				 pEntry->motNodeId, i,
				 (i < pEntry->numPrimaryConns) ? "seg" : "mir",
				 conn->remoteContentId,
				 conn->cdbProc ? conn->cdbProc->pid : 0,
				 conn->sockfd,
				 conn->remoteHostAndPort,
				 conn->localHostAndPort);
	}
}
#endif

/*
 * Set the listener address associated with the slice to
 * the master address that is established through libpq
 * connection. This guarantees that the outgoing connections
 * will connect to an address that is reachable in the event
 * when the master can not be reached by segments through
 * the network interface recorded in the catalog.
 */
void adjustMasterRouting(Slice *recvSlice)
{
	ListCell *lc = NULL;
	foreach(lc, recvSlice->primaryProcesses)
	{
		CdbProcess *cdbProc = (CdbProcess *)lfirst(lc);

		if (cdbProc->listenerAddr == NULL)
			cdbProc->listenerAddr = pstrdup(MyProcPort->remote_host);
	}
}
