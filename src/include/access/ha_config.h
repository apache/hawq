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
#ifndef _HA_CONFIG_H_
#define _HA_CONFIG_H_

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

 /*
  * An enumeration for the active namenode
  */
typedef enum HANodes
{
	HA_ONE_NODE = 1

} HANodes;

/*
 * NNHAConf - Describes the NN High-availability configuration properties
 */
typedef struct NNHAConf
{
    char            *nameservice;   /* the HA nameservice */
    char 			**nodes;		/* the  HA Namenode nodes	*/
	char			**rpcports;	    /* rpcports[0] belongs to nodes[0] */
	char			**restports;	/* restports[0] belongs to nodes[0]	*/
	int             numn;           /* number of nodes */
} NNHAConf;

NNHAConf* GPHD_HA_load_nodes(const char *nameservice);
void      GPHD_HA_release_nodes(NNHAConf *conf);

#endif	// _HA_CONFIG_H_
