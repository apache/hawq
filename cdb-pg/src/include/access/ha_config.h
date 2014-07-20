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
 * NNHAConf - Describes the NN High-availibility configuration properties 
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
