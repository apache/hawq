/*-------------------------------------------------------------------------
 *
 * seqserver.c
 *	  Process under QD postmaster used for doling out sequence values to
 * 		QEs.
 *
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
#ifndef SEQSERVER_H
#define SEQSERVER_H

#ifndef _WIN32
#include <stdint.h>             /* uint32_t (C99) */
#else
typedef unsigned int uint32_t;
#endif

#define SEQ_SERVER_REQUEST_START (0x53455152)
#define SEQ_SERVER_REQUEST_END (0x53455145)

/*
 * TODO: eventually we may want different requests to the sequence server
 * 	     other than just next_val().  at that point we should generalize
 * 		 the request/response processing code here and in the seqserver
 */
typedef struct NextValRequest
{
	uint32_t	startCookie;
	uint32_t    dbid;
	uint32_t    tablespaceid;	
	uint32_t    seq_oid;
	uint32_t    isTemp;
	uint32_t    session_id;
	uint32_t	endCookie;
}	NextValRequest;

typedef struct NextValResponse
{
	int64  	 plast;
	int64    pcached;
	int64    pincrement;
	bool 	 poverflowed;
} NextValResponse;

extern void SeqServerShmemInit(void);
extern int SeqServerShmemSize(void);
extern int seqserver_start(void);


/* used to call nextval on a sequence by going through the seqserver*/
extern void 
sendSequenceRequest(int     sockfd, 
					Relation seqrel,
                    int     session_id,
					int64  *plast, 
                    int64  *pcached,
			 		int64  *pincrement,
			 		bool   *poverflow);



#endif   /* SEQSERVER_H */
