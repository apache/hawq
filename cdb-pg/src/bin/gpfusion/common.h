/*
 * common.h
 *
 * Header for common PXF functions and utilities.
 *
 * Copyright (c) 2011, Greenplum inc
 */
#ifndef _GPHDFS_COMMON_H_
#define _GPHDFS_COMMON_H_

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/extprotocol.h"
#include "access/fileam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_exttable.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "miscadmin.h"

#include <fcntl.h>

Datum gpdbwritableformatter_export(PG_FUNCTION_ARGS);
Datum gpdbwritableformatter_import(PG_FUNCTION_ARGS);

Datum gpbridge_import(PG_FUNCTION_ARGS);

#endif
