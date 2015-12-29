/*-------------------------------------------------------------------------
 *
 * ddaserver.c
 *	  Under the QD postmaster the ddaserver combines lock information 
 *    from QEs to detect deadlocks.  Each QE has a local ddaserver that
 *    communicates with the master ddaserver at the QD when it finds
 *    a lock waiter.
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
#include "postgres.h"

#undef memcpy
#undef memset
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>


#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "cdb/cdbvars.h"
#include "commands/sequence.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/ddaserver.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"			/* PostmasterIsAlive */
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"
#include "utils/inet.h"
#include "utils/builtins.h"



#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#define SHUT_RDWR SD_BOTH
#define SHUT_RD SD_RECEIVE
#define SHUT_WR SD_SEND
#endif


 /*
  * backlog for listen() call:
  */
#define LISTEN_BACKLOG 64
#define getInt16(pNetData)	  ntohs(*((int16 *)(pNetData)))
#define getInt32(pNetData)	  ntohl(*((int32 *)(pNetData)))
#define getOid(pNetData)	



/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */


#ifdef EXEC_BACKEND
static pid_t ddaserver_forkexec(void);
#endif
NON_EXEC_STATIC void DdaserverMain(int argc, char *argv[]);

void
sendDdaRequest(int     sockfd, 
			   LockStatStd *pLk);

static void DdaserverLoop(void);
static bool processDdaRequest( int sockfd );
static int listenerSetup(void);

/*static int64 readInt64(uint8 *buff);*/
static void sendInt64(uint8 *buff, int64 i);

static void
setupDdaServerConnection(char *ddaServerHost, int ddaServerPort );


/*=========================================================================
 * GLOBAL STATE VARIABLES
 */
/* Socket file descriptor for the listener. */
static int	listenerFd;
static int	global_listenerPort;
static int	ddaserverFd;

/* our timeout value for select() and other socket operations. */
static struct timeval tval;


static bool am_ddaserver = false;
uint8 *inputBuff;

/*=========================================================================
 * VISIBLE FUNCTIONS
 */

int DdaserverShmemSize(void)
{
	
		/* just our one int4 */ /* XXX XXX XXX XXX XXX XXX */
	return sizeof(SeqServerControlBlock);
}


void DdaserverShmemInit(void)
{
/*	bool		found; */
	
}


/*
 * Main entry point for ddaserver controller process.
 *
 * This code is heavily based on pgarch.c, q.v.
 */
int
ddaserver_start(void)
{

	pid_t		DdaserverPID;

	
#ifdef EXEC_BACKEND
	switch ((DdaserverPID = ddaserver_forkexec()))
#else
	switch ((DdaserverPID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork ddaserver process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			DdaserverMain(0, NULL);
			break;
#endif
		default:
			return (int) DdaserverPID;
	}

	
	/* shouldn't get here */
	return 0;
}


/*
 * Used by QE ddaservers to send lock status to the QD ddaserver
 */
void
sendDdaRequest(int     sockfd, 
			   LockStatStd *pLk)
{	
	int		n;
	int    bytesWritten = 0;
	int	   bytesRead = 0;
	int			saved_err;
	fd_set		rset,wset;
	
	/* request message */
    LockStatStd  request;
	int     reqlen = sizeof(request);
	
	/* response message */
	int64   response;
	int     resplen = sizeof(response);
	
	
	if (sockfd == -1)
		elog( ERROR, "Lost Connection to ddaserver");
		
	/*
	 * send the sequence request to the sequence server
	 */
    memset(&request, 0, sizeof(request));	
	memcpy((void*)&request, (void*)pLk, sizeof(request));
	
	/*
	 * write the request
	 */	
	while( bytesWritten < reqlen )
	{
		
		CHECK_FOR_INTERRUPTS();
					
		n = write(sockfd, ((char *)&request)+bytesWritten, reqlen - bytesWritten);
		saved_err = errno;
	
		if (n == 0)
		{
			elog(ERROR, "Error: ddaserver socket closed.");
			return;
		}
		
		if (n < 0)
		{
			if (saved_err != EINTR && saved_err != EWOULDBLOCK)
			{
				ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
								errmsg("Error: Could not write()  message to ddaserver: %s",
								strerror(errno)),
								errdetail("error during write() call (error:%d) to ddaserver on sockfd: %d ",
											errno, sockfd)));
			}

			if (saved_err == EWOULDBLOCK)
			{
				/* we shouldn't really get here since we are dealing with
				 * small messages, but once we've read a bit of data we
				 * need to finish out reading till we get the message (or error)
				 */
				do
				{
					struct timeval timeout = tval;
					
					CHECK_FOR_INTERRUPTS();
					
					FD_ZERO(&wset);
					FD_SET(sockfd, &wset);
					n = select(sockfd + 1, NULL, &wset, NULL, &timeout);
					if (n == 0 || (n < 0 && errno == EINTR))
						continue;
					else if (n < 0)
					{
						saved_err = errno;

						ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
										errmsg("Error: Could not write()  message to ddaserver: %s",
										strerror(errno)),
										errdetail("error during write() call (error:%d) to ddaserver on sockfd: %d ",
												  errno, sockfd)));
						return;
					}
				}
				while (n < 1);
			}
			/* else saved_err == EINTR */

			continue;
		}

		bytesWritten += n;
	}
	
	/*
	 * read the response
	 */	
	while (bytesRead < resplen)
	{
		
		CHECK_FOR_INTERRUPTS();
			
		n = read(sockfd, ((char *)&response)+bytesRead, resplen - bytesRead);
		saved_err = errno;
		
		if (n == 0)
		{
			elog( ERROR, "ddaserver socket is closed");
			return;
		}
		
		if (n < 0)
		{
			if (saved_err != EINTR && saved_err != EWOULDBLOCK)
			{
				elog( ERROR, "Error: Could not read() message from ddaserver."
							  "(error:%d). Closing connection.", saved_err);
			}

			if (saved_err == EWOULDBLOCK)
			{
				/* we shouldn't really get here since we are dealing with
				 * small messages, but once we've read a bit of data we
				 * need to finish out reading till we get the message (or error)
				 */
				do
				{
					struct timeval timeout = tval;
					
					CHECK_FOR_INTERRUPTS();
					
					FD_ZERO(&rset);
					FD_SET(sockfd, &rset);
					n = select(sockfd + 1, &rset, NULL, NULL, &timeout);
					if (n == 0 || (n < 0 && errno == EINTR))
						continue;
					else if (n < 0)
					{
						saved_err = errno;

						ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
									    errmsg("Error: Could not read() message from ddaserver"),
										errdetail("error during read() call (error:%d) from remote"
												  "sockfd = %d", errno, sockfd)));
						return;		
					}
				}
				while (n < 1);
			}
			/* else saved_err == EINTR */

			continue;
		}
		else
			bytesRead += n;
	}
	
}

/*=========================================================================
 * HELPER FUNCTIONS
 */


#ifdef EXEC_BACKEND
/*
 * ddaserver_forkexec()
 *
 * Format up the arglist for the serqserver process, then fork and exec.
 */
static pid_t
ddaserver_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkddaserver";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif   /* EXEC_BACKEND */



/*
 * AutoVacMain
 */
NON_EXEC_STATIC void
DdaserverMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;
	bool bPM = AmIMaster();  

	IsUnderPostmaster = true;
	am_ddaserver = true;
	
	/* reset MyProcPid */
	MyProcPid = getpid();
	
	/* Lose the postmaster's on-exit routines */
	on_exit_reset();

	/* Identify myself via ps */
	init_ps_display("ddaserver process", "", "", "");

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.	We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 *
	 * Currently, we don't pay attention to postgresql.conf changes that
	 * happen during a single daemon iteration, so we can ignore SIGHUP.
	 */
	pqsignal(SIGHUP, SIG_IGN);

	/*
	 * Presently, SIGINT will lead to autovacuum shutdown, because that's how
	 * we handle ereport(ERROR).  It could be improved however.
	 */
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, quickdie);
	pqsignal(SIGALRM, handle_sig_alarm);

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	/* We don't listen for async notifies */
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();

	/* See InitPostgres()... */
    InitProcess();	
	InitBufferPoolBackend();
	InitXLOGAccess();

	SetProcessingMode(NormalProcessing);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * We can now go away.	Note that because we'll call InitProcess, a
		 * callback will be registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	/* set up a listener port and put it in shmem*/
/*	ddaserverCtl->ddaserverPort = listenerSetup(); */
	if (bPM)
	{
		global_listenerPort = listenerSetup(); 
		elog(LOG, "ddaserver: listener port ( %d )", global_listenerPort);
	}
	else
	{
		ddaserverFd = -1;
		global_listenerPort = -1;
	}
	
	/*
	 * setup scratch buffers
	 */
	inputBuff = palloc( sizeof(LockStatStd) );	 
	
	
	/* main loop */
	DdaserverLoop();
	

	/* One iteration done, go away */
	proc_exit(0);
}



void DdaserverLoop(void)
{
	int			n,
				highsock = 0,
				newsockfd;
	fd_set		rset,
				rrset;
	
	struct sockaddr_in addr;
	socklen_t	addrlen;
	
	List   *connectedSockets = NIL;
	ListCell   *cell;
	bool bPM = AmIMaster();  



	
	tval.tv_sec = 3;
	tval.tv_usec = 500000;
	
				
	FD_ZERO(&rset);
	FD_SET(listenerFd, &rset);
	highsock = listenerFd + 1;
	
	
	/* we'll handle many incoming sockets but keep the sockets in blocking
	 * mode since we are dealing with very small messages.
	 */
	while(true)
	{
		struct timeval timeout = tval;
		int doit=0;

		
		CHECK_FOR_INTERRUPTS();
		
		/* no need to live on if postmaster has died */
		if (!PostmasterIsAlive(true))
			exit(1);

		if (doit)
		{
			LockData* pLk;
			LockStatStd lf;
			int ii = 0;

			pLk = GetLockStatusData();
			MemSet(&lf, 0, sizeof(lf));

			if (!bPM)
			{
				if (ddaserverFd < 0)
				{
					int ddaport = 4;

					setupDdaServerConnection(
							"127.0.0.1", /* localhost */
							ddaport);
				}
			}

			while (ii >= 0)
			{
				ii = LockStatStuff(&lf, ii, pLk);

				/* send lock to qd dda */
				sendDdaRequest(ddaserverFd,
							   &lf);

				MemSet(&lf, 0, sizeof(lf));
			}

			doit = 0;


		}


		if (!bPM)
		{
			pg_usleep(10 * 1000000L);
			continue; /* XXX XXX XXX XXX */
		}		

		memcpy(&rrset, &rset, sizeof(fd_set));
		
		n = select(highsock + 1, &rrset, NULL, NULL, &timeout);
		if (n == 0)
		{
			/* timeout: Have we been here too long ? */			
			continue;
		}
		
		
		if (n < 0)
		{
			/* this may be a little severe, but if we error on select() 
			 * we'll just go ahead and blow up.  This will result in the 
			 * postmaster re-spawning a new process. 
			 */
			elog(ERROR,"Error during select() call (error:%d).", errno);
			break;	
		}
		
		elog(DEBUG5, "ddaserver select() returned %d ready sockets", n);

		/* is it someone tickling our listener port? */
		if (FD_ISSET(listenerFd, &rrset)) 
		{
			elog(DEBUG3, "ddaserver: accepting an Incoming Connection ");
            addrlen = sizeof(addr);
			if ((newsockfd = accept(listenerFd, (struct sockaddr *) & addr, &addrlen)) < 0)
			{
				/*
				 * TODO: would be nice to read the errno and try and provide
				 * more useful info as to why this happened.
				 */
				elog(LOG, "ddaserver: error during accept() call (error:%d)", errno);
			}
			
			/* make socket non-blocking BEFORE we connect. */
			if (!pg_set_noblock(newsockfd))
			{
				/*
				 * TODO: would be nice to read the errno and try and provide
				 * more useful info as to why this happened.
				 */
				elog(DEBUG3, "ddaserver: Could not set outbound socket to non-blocking mode"
					 			"error during fcntl() call (error:%d) for sockfd: %d",
							   errno, newsockfd);
			}
			
			if (newsockfd > highsock)
				highsock = newsockfd + 1;
			
			FD_SET(newsockfd, &rset);
			
			connectedSockets = lappend_int(connectedSockets, newsockfd);
						
		}
		
		
		/* loop through all of our established sockets */					
		cell = list_head(connectedSockets);
		while (cell != NULL)
		{
			int	fd = lfirst_int(cell);
			
			/* get the next cell ready before we delete */			
			cell = lnext(cell);
			
			if (FD_ISSET(fd, &rrset))
			{				
				if(!processDdaRequest(fd)) 
				{
					/* close it down */	
					FD_CLR( fd, &rset);
					connectedSockets = list_delete_int(connectedSockets, fd);
					shutdown(fd, SHUT_WR);
					close(fd);
				}							
			}	
		}
		
		
	} /* end server loop */
	
}



/*
 * Used by DdaServer to process incoming deadlock Requests
 */
static bool
processDdaRequest( int sockfd )
{
	LockStatStd ddaRequest;
    int64       ddaResponse;
	int			saved_err;
	fd_set		rset,wset;
	int n;
	int bytesRead = 0;
	int bytesWritten = 0;
	int reqlen = sizeof(LockStatStd);
    int outputBuffLength = sizeof(LockStatStd);
	
	
	/* Read in the incoming request message */
	while( bytesRead < reqlen )
	{
		
		CHECK_FOR_INTERRUPTS();
			
		n = read(sockfd, inputBuff + bytesRead, reqlen - bytesRead);
		saved_err = errno;
		
		if (n == 0)
		{
			elog( DEBUG3, "client socket is closed");
			return false;
		}
		
		if (n < 0)
		{
			if (saved_err != EINTR && saved_err != EWOULDBLOCK)
			{
				elog( DEBUG3, "Error: Could not read() message from client."
					  "(error:%d). Closing connection.", saved_err);
				return false;
			}

			if (saved_err == EWOULDBLOCK)
			{
				/* we shouldn't really get here since we are dealing with
				 * small messages, but once we've read a bit of data we
				 * need to finish out reading till we get the message (or error)
				 */
				do
				{
					struct timeval timeout = tval;
					
					CHECK_FOR_INTERRUPTS();
					
					FD_ZERO(&rset);
					FD_SET(sockfd, &rset);
					n = select(sockfd + 1, &rset, NULL, NULL, &timeout);
					if (n == 0 || (n < 0 && errno == EINTR))
						continue;
					else if (n < 0)
					{
						saved_err = errno;

						elog(DEBUG3, "error reading incoming message from client. (errno: %d)",
							 saved_err);
						return false;
					}
				}
				while (n < 1);
				
			}
			/* else saved_err == EINTR */
			continue;
		}

		bytesRead += n;
	}
	
	
	/*
	 * process what we've read
	 */
    memcpy(&ddaRequest, inputBuff, sizeof(ddaRequest));
/*
	ddaRequest.dbid    	    = ntohl(ddaRequest.dbid);
	ddaRequest.tablespaceid = ntohl(ddaRequest.tablespaceid);
	ddaRequest.seq_oid      = ntohl(ddaRequest.seq_oid);
	ddaRequest.isTemp       = ntohl(ddaRequest.isTemp);
    ddaRequest.session_id   = ntohl(nextValRequest.session_id);
	
	elog( DEBUG5, "Received nextval request for dbid: %ld tablespaceid: %ld seqoid: "
				  "%ld isTemp: %s session_id: %ld",
				  (long int)nextValRequest.dbid,
				  (long int)nextValRequest.tablespaceid,
				  (long int)nextValRequest.seq_oid,
				  nextValRequest.isTemp?"true":"false",
				  (long)nextValRequest.session_id);
	
*/
	/*
	 * Process request
	 */
/*	cdb_sequence_nextval_server(nextValRequest.tablespaceid,
    	                        nextValRequest.dbid,
        	                    nextValRequest.seq_oid,
            	                nextValRequest.isTemp,
                	            &plast,
                    	        &pcached,
                        	    &pincrement);
*/
	
	/*
	 * Send the response
	 */
	sendInt64((uint8 *)&ddaResponse, 42);

	/*
	 * Write the response
	 */
	while(bytesWritten < outputBuffLength )
	{
		
		CHECK_FOR_INTERRUPTS();
					
		n = write(sockfd, ((uint8 *)&ddaResponse)+bytesWritten, outputBuffLength - bytesWritten);
		saved_err = errno;
	
		if (n == 0)
		{
			elog( DEBUG3, "client socket is closed");
			return false;
		}
		
		if (n < 0)
		{
			if (saved_err != EINTR && saved_err != EWOULDBLOCK)
			{
				elog( DEBUG3, "Error: Could not write() message from client."
					  "(error:%d). Closing connection.", sockfd);
				return false;
			}

			if (saved_err == EWOULDBLOCK)
			{
				/* we shouldn't really get here since we are dealing with
				 * small messages, but once we've read a bit of data we
				 * need to finish out reading till we get the message (or error)
				 */
				do
				{
					struct timeval timeout = tval;
					
					CHECK_FOR_INTERRUPTS();
					
					FD_ZERO(&wset);
					FD_SET(sockfd, &wset);
					n = select(sockfd + 1, NULL, &wset, NULL, &timeout);
					if (n == 0 || (n < 0 && errno == EINTR))
						continue;
					else if (n < 0)
					{
						saved_err = errno;

						elog( DEBUG3, "error writing incoming message from client. (errno: %d)", saved_err);
						return false;
					}
				}
				while (n < 1);				
			}
			/* else saved_err == EINTR */
			continue;
		}
		else
			bytesWritten += n;
	}
	
	return true;
}

/* listenerSetup
 *
 * Sets up a listener on a system-picked port.  Returns the port that the
 * system port.
 */
static int
listenerSetup(void)
{
	int listenerPort;

	struct sockaddr_storage addr;
	socklen_t	addrlen;

    struct addrinfo hints;
	struct addrinfo *addrs, *rp;
	int  s;
	char service[32];

	/*
	 * we let the system pick the TCP port here so we don't have to
	 * manage port resources ourselves.
	 */
    snprintf(service,32,"%d",0);
    memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;    	/* Allow IPv4 or IPv6 */
	hints.ai_socktype = SOCK_STREAM; 	/* TCP socket */
	hints.ai_flags = AI_PASSIVE;    	/* For wildcard IP address */
	hints.ai_protocol = 0;          	/* Any protocol */

	s = getaddrinfo(NULL, service, &hints, &addrs);
	if (s != 0)
		elog(ERROR,"getaddrinfo says %s",gai_strerror(s));

	/*
	 * getaddrinfo() returns a list of address structures,
	 * one for each valid address and family we can use.
	 *
	 * Try each address until we successfully bind.
	 * If socket (or bind) fails, we (close the socket
	 * and) try the next address.  This can happen if
	 * the system supports IPv6, but IPv6 is disabled from
	 * working, or if it supports IPv6 and IPv4 is disabled.
	 */

	/*
	 * If there is both an AF_INET6 and an AF_INET choice,
	 * we prefer the AF_INET6, because on UNIX it can receive either
	 * protocol, whereas AF_INET can only get IPv4.  Otherwise we'd need
	 * to bind two sockets, one for each protocol.
	 *
	 * Why not just use AF_INET6 in the hints?  That works perfect
	 * if we know this machine supports IPv6 and IPv6 is enabled,
	 * but we don't know that.
	 */

#ifdef HAVE_IPV6
	if (addrs->ai_family == AF_INET && addrs->ai_next != NULL && addrs->ai_next->ai_family == AF_INET6)
	{
		/*
		 * We got both an INET and INET6 possibility, but we want to prefer the INET6 one if it works.
		 * Reverse the order we got from getaddrinfo so that we try things in our preferred order.
		 * If we got more possibilities (other AFs??), I don't think we care about them, so don't
		 * worry if the list is more that two, we just rearrange the first two.
		 */
		struct addrinfo *temp = addrs->ai_next; 	/* second node */
		addrs->ai_next = addrs->ai_next->ai_next; 	/* point old first node to third node if any */
		temp->ai_next = addrs;   					/* point second node to first */
		addrs = temp;								/* start the list with the old second node */
	}
#endif

	for (rp = addrs; rp != NULL; rp = rp->ai_next)
	{
		/*
		 * getaddrinfo gives us all the parameters for the socket() call
		 * as well as the parameters for the bind() call.
		 */
		elog(DEBUG1,"seqserver  listener socket %d %d %d",rp->ai_family, rp->ai_socktype, rp->ai_protocol);

		listenerFd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (listenerFd == -1)
			continue;

		if (bind(listenerFd, rp->ai_addr, rp->ai_addrlen) == 0)
			break;              /* Success */

		close(listenerFd);
	}

	if (rp == NULL)
	{               /* No address succeeded */
		ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
			   errmsg("SeqServer Error: Could not setup listener socket: %m"),
				  errdetail("error during bind() call (error:%d).", errno)));
		
	}

	if (listen(listenerFd, LISTEN_BACKLOG) < 0)
	{
		ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("SeqServer Error: Could not setup listener socket: %s", strerror(errno)),
				errdetail("error during listen() call (error:%d).", errno)));
	}


	addrlen = sizeof(addr);

	if (getsockname(listenerFd, (struct sockaddr *) & addr, &addrlen) < 0)
	{
		ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("SeqServer Error: Could not setup listener socket: %s", strerror(errno)),
		   errdetail("error during getsockname() call (error:%d).", errno)));
	}


	/* display which port was chosen by the system. */
	if (addr.ss_family == AF_INET6)
		listenerPort = ntohs(((struct sockaddr_in6*)&addr)->sin6_port);
	else
		listenerPort = ntohs(((struct sockaddr_in*)&addr)->sin_port);

	elog(DEBUG1, "Ddaserver listener on port: %d", listenerPort);

	return listenerPort;
}

/*
int64 readInt64( uint8 *buff )
{
	int64		result;

	memcpy(&result, buff, 8 );	
		
	return result;
}
*/

void sendInt64(uint8 *buff, int64 i)
{
	memcpy( buff, &i, 8);
}


/* from ml_ipc.c */
static void
setupDdaServerConnection(char *ddaServerHost, int ddaServerPort )
{
	int  n;
	struct sockaddr_in addr;	
	
	
	/*
	 * open a connection to the sequence server
	 */
	ddaserverFd = socket(AF_INET, SOCK_STREAM, 0);
	
	
	addr.sin_family = AF_INET;

	if ((n = inet_net_pton(AF_INET, ddaServerHost, &addr.sin_addr, sizeof(addr.sin_addr))) < 1)
	{
		if (n == 0)
		{
			ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("Interconnect Error: Could not parse remote ddaserver"
						"address: '%s'", ddaServerHost),
					   errdetail("inet_pton() unable to parse address: '%s'",
								 ddaServerHost)));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
			 errmsg("Interconnect Error: System error occured during parsing"
				 " of remote ddaserver address: '%s'", ddaServerHost),
							errdetail("inet_pton()  (errno: %d) unable to parse address: '%s'",
									  errno, ddaServerHost)));
		}
	}

	addr.sin_port = htons(ddaServerPort);

	if ((n = connect(ddaserverFd, (struct sockaddr *) & addr, sizeof(addr))) < 0)
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						errmsg("Interconnect Error: Could not connect to ddaserver."),
		                errdetail("%m%s", "connect")
                ));
	
	/* make socket non-blocking BEFORE we connect. */
	if (!pg_set_noblock(ddaserverFd))
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						errmsg("Interconnect Error: Could not set ddaserver socket"
						       "to non-blocking mode."),
			            errdetail("%m%s sockfd=%d", "fcntl", ddaserverFd)
                ));
	
}



