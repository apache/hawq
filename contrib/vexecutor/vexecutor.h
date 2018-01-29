#ifndef __VEXECUTOR_H__
#define __VEXECUTOR_H__

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "cdb/cdbparquetfooterprocessor.h"
#include "cdb/cdbparquetfooterserializer.h"
#include "cdb/cdbparquetrowgroup.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "snappy-c.h"
#include "zlib.h"
#include "executor/spi.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"

/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);

#endif
