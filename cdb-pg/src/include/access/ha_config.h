#ifndef _HA_CONFIG_H_
#define _HA_CONFIG_H_

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

/*
 * An enumeration for the active namenode
 */
typedef enum ActiveNN
{
	AC_NOT_SET = -1,
	AC_ONE_NODE = 1
	
} ActiveNN;

/*
 * NNHAConf - Describes the NN High-availibility configuration properties 
 */
typedef struct NNHAConf
{
    char 			**nodes;		/* the  HA Namenode nodes	*/
	char			**rpcports;	    /* rpcports[0] belongs to nodes[0] */
	char			**restports;	/* restports[0] belongs to nodes[0]	*/
	int             active;         /* the active host */
	int             numn;           /* number of nodes */
} NNHAConf;

NNHAConf* load_nn_ha_config(const char *nameservice);
void      release_nn_ha_config(NNHAConf *conf);

#endif	// _HA_CONFIG_H_
