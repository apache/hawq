#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "hdfs/hdfs.h"

/* Mock functions for the hdfs.h APIs used by load_nn_ha_config() in ha_config.c */

Namenode * hdfsGetHANamenodes(const char *nameservice, int *size)
{
	optional_assignment(size);
	return (Namenode *)mock();
}

void hdfsFreeNamenodeInformation(Namenode *namenodes, int size)
{
	mock();
}

hdfsFS hdfsConnect(const char *host, uint16_t port)
{
	return (hdfsFS)mock();
}

int hdfsDisconnect(hdfsFS fileSystem)
{
	mock();
}