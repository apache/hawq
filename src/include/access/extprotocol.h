/*-------------------------------------------------------------------------
 *
 * access/exttableprotocol.h
 *	  Declarations for External Table Protocol functions
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

#ifndef EXTPROTOCOL_H
#define EXTPROTOCOL_H

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "utils/rel.h"

/* ------------------------- I/O function API -----------------------------*/

struct ExternalSelectDescData;
typedef struct ExternalSelectDescData *ExternalSelectDesc;
/*
 * ExtProtocolData is the node type that is passed as fmgr "context" info
 * when a function is called by the External Table protocol manager.
 */
typedef struct ExtProtocolData
{
	NodeTag			type;                 /* see T_ExtProtocolData */
	Relation    	prot_relation;
	char		   *prot_url;
	char		   *prot_databuf;
	int				prot_maxbytes;
	void		   *prot_user_ctx;
	bool			prot_last_call;
	List		   *prot_scanquals;
	ExternalSelectDesc desc;

} ExtProtocolData;

typedef ExtProtocolData *ExtProtocol;

#define CALLED_AS_EXTPROTOCOL(fcinfo) \
	((fcinfo->context != NULL && IsA((fcinfo)->context, ExtProtocolData)))

#define EXTPROTOCOL_GET_URL(fcinfo)    	   (((ExtProtocolData*) fcinfo->context)->prot_url)
#define EXTPROTOCOL_GET_RELATION(fcinfo)   (((ExtProtocolData*) fcinfo->context)->prot_relation)
#define EXTPROTOCOL_GET_DATABUF(fcinfo)    (((ExtProtocolData*) fcinfo->context)->prot_databuf)
#define EXTPROTOCOL_GET_DATALEN(fcinfo)    (((ExtProtocolData*) fcinfo->context)->prot_maxbytes)
#define EXTPROTOCOL_GET_SCANQUALS(fcinfo)    (((ExtProtocolData*) fcinfo->context)->prot_scanquals)
#define EXTPROTOCOL_GET_USER_CTX(fcinfo)   (((ExtProtocolData*) fcinfo->context)->prot_user_ctx)
#define EXTPROTOCOL_GET_SELECTDESC(fcinfo)   (((ExtProtocolData*) fcinfo->context)->desc)
#define EXTPROTOCOL_GET_PROJINFO(fcinfo) (((ExtProtocolData*) fcinfo->context)->desc->projInfo)
#define EXTPROTOCOL_IS_LAST_CALL(fcinfo)   (((ExtProtocolData*) fcinfo->context)->prot_last_call)

#define EXTPROTOCOL_SET_LAST_CALL(fcinfo)  (((ExtProtocolData*) fcinfo->context)->prot_last_call = true)
#define EXTPROTOCOL_SET_USER_CTX(fcinfo, p) \
	(((ExtProtocolData*) fcinfo->context)->prot_user_ctx = p)

/* ------------------------- Validator function API -----------------------------*/

typedef enum ValidatorDirection
{
	EXT_VALIDATE_READ,
	EXT_VALIDATE_WRITE
} ValidatorDirection;

/*
 * ExtProtocolValidatorData is the node type that is passed as fmgr "context" info
 * when a function is called by the External Table protocol manager.
 */
typedef struct ExtProtocolValidatorData
{
	NodeTag				 type;            /* see T_ExtProtocolValidatorData */
	List 		  		*url_list;
	List 		  		*format_opts;
	ValidatorDirection 	 direction;  /* validating read or write? */
	char				*errmsg;		  /* the validation error upon return, if any */
	
} ExtProtocolValidatorData;

typedef ExtProtocolValidatorData *ExtProtocolValidator;

#define CALLED_AS_EXTPROTOCOL_VALIDATOR(fcinfo) \
	((fcinfo->context != NULL && IsA((fcinfo)->context, ExtProtocolValidatorData)))

#define EXTPROTOCOL_VALIDATOR_GET_URL_LIST(fcinfo)	(((ExtProtocolValidatorData*) fcinfo->context)->url_list)
#define EXTPROTOCOL_VALIDATOR_GET_NUM_URLS(fcinfo)	(list_length(((ExtProtocolValidatorData*) fcinfo->context)->url_list))

#define EXTPROTOCOL_VALIDATOR_GET_NTH_URL(fcinfo, n) (((Value *)(list_nth(EXTPROTOCOL_VALIDATOR_GET_URL_LIST(fcinfo),(n - 1))))->val.str)

#define EXTPROTOCOL_VALIDATOR_GET_FMT_OPT_LIST(fcinfo)  (((ExtProtocolValidatorData*) fcinfo->context)->format_opts)
#define EXTPROTOCOL_VALIDATOR_GET_NUM_FMT_OPTS(fcinfo)  (list_length(((ExtProtocolValidatorData*) fcinfo->context)->format_opts))
#define EXTPROTOCOL_VALIDATOR_GET_NTH_FMT_OPT(fcinfo, n) (((Value *)(list_nth(EXTPROTOCOL_VALIDATOR_GET_FMT_OPT_LIST(fcinfo),(n - 1))))->val.str)

#define EXTPROTOCOL_VALIDATOR_GET_DIRECTION(fcinfo)	(((ExtProtocolValidatorData*) fcinfo->context)->direction)


#endif /* EXTPROTOCOL_H */
