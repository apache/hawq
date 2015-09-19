#ifndef DYNAMIC_RESOURCE_MANAGEMENT_CONNECTION_TRACK_H
#define DYNAMIC_RESOURCE_MANAGEMENT_CONNECTION_TRACK_H
#include "resourcemanager/envswitch.h"
#include "resourcemanager/utils/linkedlist.h"
#include "resourcemanager/utils/hashtable.h"
#include "resourcemanager/communication/rmcomm_AsyncComm.h"

/*******************************************************************************
 *
 * Transformation of the connection tracker status.
 *
 * CONN_PP_INFO_NOTSET initial value
 *
 * CONN_PP_INFO_NOTSET 					---(connection accepted						)---> CONN_PP_ESTABLISHED
 *
 * CONN_PP_ESTABLISHED 					---(register succeeds						)---> CONN_PP_REGISTER_DONE
 * CONN_PP_ESTABLISHED 					---(register fails due to wrong args		)---> CONN_PP_REGISTER_FAIL
 * CONN_PP_ESTABLISHED 					---(no action timeout						)---> CONN_PP_TIMEOUT_FAIL
 *
 * CONN_PP_REGISTER_DONE 				---(acquire resource with valid args		)---> CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT
 * CONN_PP_REGISTER_DONE 				---(acquire resource fails due to wrong args)---> CONN_PP_RESOURCE_ACQUIRE_FAIL
 * CONN_PP_REGISTER_DONE 				---(no action timeout						)---> CONN_PP_TIMEOUT_FAIL
 * CONN_PP_REGISTER_DONE				---(ddl request succeeds with valid args	)---> CONN_PP_DDL_REQUEST_ACCEPTED
 * CONN_PP_REGISTER_DONE				---(unregister the connection               )---> CONN_PP_ESTABLISHED
 *
 * CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT 	---(dispatched resource						)---> CONN_PP_RESOURCE_QUEUE_ALLOC_DONE
 * CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT    ---(fail to dispatch resource				)---> CONN_PP_RESOURCE_QUEUE_ALLOC_FAIL
 * CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT    ---(wait resource timeout					)---> CONN_PP_TIMEOUT_FAIL
 *
 * CONN_PP_RESOURCE_QUEUE_ALLOC_DONE    ---(return succeeds							)---> CONN_PP_REGISTER_DONE
 * CONN_PP_RESOURCE_QUEUE_ALLOC_DONE    ---(no action timeout						)---> CONN_PP_TIMEOUT_FAIL
 *
 * CONN_PP_DDL_REQUEST_ACCEPTED			---(ddl operation is done regardless result	)---> CONN_PP_REGISTER_DONE
 *
 *******************************************************************************/

enum CONN_PROCESS_PROGRESSES {
	CONN_PP_INFO_NOTSET,
	CONN_PP_ESTABLISHED,
	CONN_PP_REGISTER_DONE,
	CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT,
	CONN_PP_RESOURCE_QUEUE_ALLOC_DONE,
	CONN_PP_DDL_REQUEST_ACCEPTED,

	CONN_PP_FAILS,       /* All states greater than this are failing status. */

	CONN_PP_REGISTER_FAIL,
	CONN_PP_RESOURCE_ACQUIRE_FAIL,
	CONN_PP_RESOURCE_QUEUE_ALLOC_FAIL,
	CONN_PP_TIMEOUT_FAIL,
	CONN_PP_TRANSFORM_ERROR
};

#define INVALID_CONNID		-1

struct DynResourceQueueTrackData;

struct ConnectionTrackData
{
	uint64_t				ConnectTime;   /* When connection is created.	  */
	uint64_t				RegisterTime;  /* When connection registered. 	  */
	uint64_t				ResRequestTime;/* When resource allocation request
											  is received.			  		  */
	uint64_t				ResAllocTime;  /* When resource is allocated. 	  */
	uint64_t				LastActTime;   /* Last action time.			      */
	uint64_t				HeadQueueTime; /* When request is queued at head. */

	/* Socket connection information */
	struct sockaddr_in 		ClientAddr;
	socklen_t				ClientAddrLen;
	int						ClientSocket;
	char					ClientAddrDotStr[16];
	uint16_t				ClientAddrPort;

	/* Input Message content */
	uint32_t				MessageSize;
	uint8_t					MessageMark1;
	uint8_t					MessageMark2;
	uint16_t				MessageID;
	SelfMaintainBufferData	MessageBuff;
	uint64_t				MessageReceiveTime;

	int32_t					ConnID;			/* Allocated connection ID.		  */
	char  					UserID[64];		/* User ID string.		   		  */
	uint32_t				QueueID;		/* Queue ID 			   		  */

	int						Progress;		/* The processing progress.		  */
	bool			    	ResponseSent;

	int64_t					SessionID;

	int32_t					SegMemoryMB;
	double 					SegCore;
	int64_t					SegIOBytes;
	int32_t					SegNum;
	int32_t					SegNumMin;
	int32_t					SegNumActual;
	int32_t					SegPreferredHostCount;
	char 			  	  **SegPreferredHostNames;
	int64_t			   	   *SegPreferredScanSizeMB;
	int32_t					SliceSize;
	int64_t					IOBytes;
	int32_t			    	MaxSegCountFixed;
	int32_t			    	MinSegCountFixed;
	int32_t					VSegLimitPerSeg;
	int32_t					VSegLimit;
	List				   *Resource;		/* Allocated resource. 	   		  */

	void				   *QueueTrack;
	void				   *User;

	bool					isOld;			/* Connection IS OLD when resource
											   manager resets resource broker.*/

	/*
	 * When this connection track ( in resource alloc waiting progress )
	 * encountered resource fragment problem.
	 */
	bool					troubledByFragment;
	uint64_t				troubledByFragmentTimestamp;

	AsyncCommBuffer			CommBuffer;		/* Corresponding RPC communication
											   buffer if the socket connection
											   is active. 					  */
};

typedef struct ConnectionTrackData  ConnectionTrackData;
typedef struct ConnectionTrackData *ConnectionTrack;

#define HAWQRM_QD_CONNECTION_MAX_CAPABILITY 				0X10000

struct ConnectionTrackManagerData
{
	HASHTABLEData	Connections;			/* Hash table of (connid,connection)
											   for fast connection fetching.  */
	List		   *FreeConnIDs;			/* Pre-built free connection IDs. */

	List		   *ConnHavingRequests;		/* Batch request processing list. */
	List		   *ConnToSend;				/* Batch response sending list.	  */
};

typedef struct ConnectionTrackManagerData *ConnectionTrackManager;
typedef struct ConnectionTrackManagerData  ConnectionTrackManagerData;

/* Initialize connection track manager. */
void initializeConnectionTrackManager(void);

/* Use connection id. */
int useConnectionID(int32_t *connid);
/* Return connection id. */
int returnConnectionID(int32_t connid);
/* Use one empty connection track instance for current connection. */
int useConnectionTrack(ConnectionTrack *track);
/* Track connection id based on connection id. */
void trackConnectionTrack(ConnectionTrack track);
/* Return and recycle one connection track that will not be used. */
void returnConnectionTrack(ConnectionTrack track);
/* Search one in-use connection track instance. */
int getInUseConnectionTrack(uint32_t connid, ConnectionTrack *track);

int retrieveConnectionTrack(ConnectionTrack track, int32_t connid);

void transformConnectionTrackProgress(ConnectionTrack 			   track,
									  enum CONN_PROCESS_PROGRESSES progress);
bool canTransformConnectionTrackProgress(ConnectionTrack 			  track,
									  	 enum CONN_PROCESS_PROGRESSES progress);

void processSubmittedRequests(void);

void buildSegPreferredHostInfo(ConnectionTrack track);
void freeSegPreferredHostInfo(ConnectionTrack track);

void setAllAllocatedResourceInConnectionTracksOld(void);

void dumpConnectionTracks(const char *filename);

/* Build response message into Connection Track instance. */
void buildResponseIntoConnTrack(ConnectionTrack  conntrack,
				   	   	   	    char 			*buf,
								uint32_t 		 bufsize,
								uint8_t  		 mark1,
								uint8_t  		 mark2,
								uint16_t 		 messageid);
#endif /*DYNAMIC_RESOURCE_MANAGEMENT_CONNECTION_TRACK_H*/
