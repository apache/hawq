/*
 * execIndexScan.h
 *
 * Copyright (c) 2013 - present, EMC/Greenplum
 */
#ifndef EXECINDEXSCAN_H
#define EXECINDEXSCAN_H

#include "nodes/execnodes.h"


extern void
InitCommonIndexScanState(IndexScanState *indexstate, IndexScan *node, EState *estate, int eflags);

#endif
