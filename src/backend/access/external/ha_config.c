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

#include "access/ha_config.h"
#include "access/pxfcomutils.h"
#include "access/pxfutils.h"
#include "hdfs/hdfs.h"
#include "utils/guc.h"

#define ALLOC_STRINGS_ARR(sz) ((char**)palloc0(sizeof(char*) * sz))

static NNHAConf *init_config(unsigned int numnodes, const char *nameservice);
static NNHAConf *load_hdfs_client_config(const char *nameservice);
static void free_string_array(char **arr, int size);
static void set_one_namenode(NNHAConf *conf, int idx, Namenode *source);
static void validate_result(NNHAConf *conf);
static void validate_port(char *port,  const char *m1, int num);
static void validate_string(char *s,  const char *m1, int num);
static void traceNamenodeArr(Namenode* nns, int len);

/*
 * GPHD_HA_load_nodes
 *
 * Load the Namenode High-Availability properties set for a given 
 * HA nameservice from the HDFS client configuration files
 * TODO:
 * This is a temporary solution that will be removed once the PXF servlet
 * will stop using the HDFS namenode/datanodes as a hosting application
 * server and will move to an independent stand-alone application server 
 */
NNHAConf*
GPHD_HA_load_nodes(const char *nameservice)
{
	NNHAConf *conf = load_hdfs_client_config(nameservice);
	validate_result(conf);
	return conf;
}

/*
 * GPHD_HA_release_nodes
 *
 * Free the memory allocated for the data structure holding the HA configuration
 */
void
GPHD_HA_release_nodes(NNHAConf *conf)
{
	if (!conf)
		return;
	
	pfree(conf->nameservice);
	free_string_array(conf->nodes, conf->numn);
	free_string_array(conf->rpcports, conf->numn);
	free_string_array(conf->restports, conf->numn);
	pfree(conf);
}

/*
 * Free one strings array field
 */
static void 
free_string_array(char **arr, int size)
{
	int i;
	
	if (!arr)
		return;
	
	for (i = 0; i < size; i++)
	{
		if(arr[i]) 
			pfree(arr[i]);
	}
	pfree(arr);
}

/*
 * Initialize NNHAConf structure
 */
static NNHAConf* 
init_config(unsigned int numnodes, const char *nameservice)
{
	NNHAConf	*conf = (NNHAConf *)palloc0(sizeof(NNHAConf));
	
	conf->nameservice   = pstrdup(nameservice);
	conf->nodes         = ALLOC_STRINGS_ARR(numnodes);
	conf->rpcports      = ALLOC_STRINGS_ARR(numnodes);
	conf->restports     = ALLOC_STRINGS_ARR(numnodes);
	conf->numn          = numnodes;
	
	return conf;
}

/*
 * load the HDFS client configuration
 */
static NNHAConf* 
load_hdfs_client_config(const char *nameservice)
{
	int len, i;
	NNHAConf *conf;
	Namenode *nns = hdfsGetHANamenodes(nameservice, &len);
	
	if (nns == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("nameservice %s not found in client configuration. No HA namenodes provided",
						nameservice)));
	
	if (len == HA_ONE_NODE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("High availability for nameservice %s was configured with only one node. A high availability scheme requires at least two nodes ",
						nameservice)));
		
	conf = init_config(len, nameservice);
	
	for (i = 0; i < conf->numn; i++)
		set_one_namenode(conf, i, &nns[i]);
	
	/* 
	 * If we succeeded to create NNHAConf from the input Namenode array in set_one_namenode(), 
	 * we can at least say that the Namenode array has no NULL or empty strings inside.
	 * Then we can safely trace it to the log so we can record what was received from 
	 * the configuration.
	 */
	traceNamenodeArr(nns, len);
	
	hdfsFreeNamenodeInformation(nns, len);
	
	return conf;
}

/*
 * Trace the Namenode array to the log
 */
static void 
traceNamenodeArr(Namenode* nns, int len)
{
	int i;
	for (i = 0; i < len; i++)
		elog(DEBUG2, "PXF received from configuration HA Namenode-%d having rpc-address <%s> and rest-address <%s>", 
			 i + 1, nns[i].rpc_addr, nns[i].http_addr);
}

/*
 * Translates the Namenode structure to a NNHAConf structure
 * Input Namenode:
 *	rpc_addr: mdw:9000
 *	http_addr: mdw:50070
 */
static void
set_one_namenode(NNHAConf *conf, int idx, Namenode *source)
{
	char *portstart;
	int hostlen;
	
	validate_string(source->rpc_addr, "In configuration Namenode.rpc_address number %d is null or empty", idx + 1);
	portstart = strchr(source->rpc_addr, ':');
	if (!portstart)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("dfs.namenode.rpc-address was set incorrectly in the configuration. ':' missing")));	
		
	hostlen = portstart - source->rpc_addr;
	portstart++;
	
	conf->nodes[idx] = pnstrdup(source->rpc_addr, hostlen);
	conf->rpcports[idx] = pstrdup(portstart);
	
/*
 * we override the ha rest post from hdfs-site.xml, with pxf_service_port
 */
	port_to_str(&(conf->restports[idx]), pxf_service_port);
}

/*
 * Validate the obtained NN host and ports
 */
static void 
validate_result(NNHAConf *conf)
{	
	for (int i = 0; i < conf->numn; i++)
	{
		validate_string(conf->nodes[i], "HA Namenode host number %d is NULL value", i + 1);
		validate_port(conf->rpcports[i], "HA Namenode RPC port number %d is NULL value", i + 1);
		validate_port(conf->restports[i], "HA Namenode REST port number %d is NULL value", i + 1);
	}
}

/*
 * Validate string
 */
static void
validate_string(char *s, const char *m1, int num) 
{
	if (!s || strlen(s) == 0) 
		ereport(ERROR, 
				(errcode(ERRCODE_SYNTAX_ERROR), 
				 errmsg(m1, num))); /* splitting the string into m1 and m2 is a hack to make errmsg accept a string parameter instead of a literal */
}

/* 
 * make sure port is in the valid numbers range for a port
 */
static void 
validate_port(char *port,  const char *m1, int num)
{
	const long  max_port_number = 65535;
	long numport;
	char *tail = NULL;
	
	validate_string(port, m1, num);
	
	/*now validate number */
	numport = strtol(port, &tail, 10); /* atol and atoi will not catch 100abc. They will return 100 */
	if (numport == 0 || (tail && strlen(tail) > 0) /* port had a non-numeric part*/ || numport > max_port_number)
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid port <%s> detected in nameservice configuration", port)));
}
