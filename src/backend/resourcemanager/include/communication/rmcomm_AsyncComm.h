#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_ASYNCCOMM_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_ASYNCCOMM_H

#include "resourcemanager/envswitch.h"

extern MCTYPE AsyncCommContext;

struct AsyncCommBufferData;
typedef struct AsyncCommBufferData  AsyncCommBufferData;
typedef struct AsyncCommBufferData *AsyncCommBuffer;

typedef void ( * PollReadyHandlerType)(AsyncCommBuffer);

struct AsyncCommBufferHandlersData {
	PollReadyHandlerType    InitHandle;			/* After creating this buff   */
	PollReadyHandlerType    ReadReadyHandle;	/* Before reading new data    */
	PollReadyHandlerType   	ReadPostHandle;		/* After reading new data     */
	PollReadyHandlerType   	WriteReadyHandle;	/* Before writing data out    */
	PollReadyHandlerType	WritePostHandle;	/* After writing data         */
	PollReadyHandlerType   	ErrorReadyHandle;   /* When POLLERR or POLLHUP set*/
	PollReadyHandlerType	CleanUpHandle;		/* Before freeing this buffer */
};

typedef struct AsyncCommBufferHandlersData  AsyncCommBufferHandlersData;
typedef struct AsyncCommBufferHandlersData *AsyncCommBufferHandlers;

#define ASYNCCOMM_READ 		 0X00000001 /* Handle read ready action. */
#define ASYNCCOMM_READBYTES  0X00000002 /* Should read bytes from connection and handle it. */
#define ASYNCCOMM_WRITE		 0X00000004 /* Handle write ready action. */
#define ASYNCCOMM_WRITEBYTES 0X00000008	/* Should write bytes to connection. */

struct AsyncCommBufferData {
	int						 FD;
	char					*DomainFileName;
	SelfMaintainBufferData 	 ReadBuffer;
	List 		 			*WriteBuffer;
	/* Complete content size track. */
	int						 WriteContentSize;
	int						 WriteContentOriginalSize;

	uint32_t				 ActionMask;

	/* If should actively close. */
	bool				  	 toClose;
	/* If should close without handling left data. */
	bool					 forcedClose;
	void				   	*UserData;
	AsyncCommBufferHandlers	 Methods;

	/* Forced error action.   */
	int						 forceErrorAction;
};

/* Initialize the asynchronous communication. */
void initializeAsyncComm(void);

/* Register one file descriptor for a connected socket connection. */
int registerFileDesc(int 					  fd,
					 char					 *dmfilename,
					 uint32_t				  actionmask,
					 AsyncCommBufferHandlers  methods,
					 void 					 *userdata,
					 AsyncCommBuffer         *newcommbuffer);

/* Register one comm buffer for asynchronous connection and communication. */
int registerAsyncConnectionFileDesc(const char				*sockpath,
									const char				*address,
									uint16_t				 port,
									uint32_t				 actionmask,
									AsyncCommBufferHandlers  methods,
									void					*userdata,
									AsyncCommBuffer			*newcommbuffer);

/* If new fd can be registered. */
bool canRegisterFileDesc(void);
/* Process all registered file descriptors. */
int processAllCommFileDescs(void);

void closeAndRemoveAllRegisteredFileDesc(void);

void addMessageContentToCommBuffer(AsyncCommBuffer 		buffer,
								   SelfMaintainBuffer 	content);

SelfMaintainBuffer getFirstWriteBuffer(AsyncCommBuffer commbuffer);

void shiftOutFirstWriteBuffer(AsyncCommBuffer commbuffer);
#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_ASYNCCOMM_H*/
