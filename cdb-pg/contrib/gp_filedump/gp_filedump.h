/*
 * pg_filedump.h - PostgreSQL file dump utility for dumping and
 *                 formatting heap(data), index and control files.
 *                 Version 8.2.0 for PostgreSQL 8.2
 * 
 * Copyright (c) 2002-2007 Red Hat, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Author: Patrick Macdonald <patrickm@redhat.com> 
 *
 * Component of: PostgreSQL - Red Hat Edition - Utilities / Tools
 * 
 */

#include "c.h"

#include <stdio.h>
#include <time.h>
#include <ctype.h>
#include <sys/stat.h>

#include "postgres.h"
#include "pg_config_manual.h"

#include "storage/bufpage.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/gin.h"
#include "access/gist.h"
#include "access/nbtree.h"
#include "access/itup.h"
#include "access/htup.h"
#include "catalog/pg_control.h"
#include "utils/builtins.h"

// Options for Block formatting operations
static unsigned int blockOptions = 0;
typedef enum
{
  BLOCK_ABSOLUTE = 0x00000001,	// -a: Absolute (vs Relative) addressing
  BLOCK_BINARY	 = 0x00000002,	// -b: Binary dump of block
  BLOCK_FORMAT	 = 0x00000004,	// -f: Formatted dump of blocks / control file
  BLOCK_FORCED	 = 0x00000008,	// -S: Block size forced
  BLOCK_NO_INTR	 = 0x00000010,	// -d: Dump straight blocks
  BLOCK_RANGE	 = 0x00000020	// -R: Specific block range to dump
}
blockSwitches;

static int blockStart = -1;	// -R [start]: Block range start 
static int blockEnd = -1;	// -R [end]: Block range end

// Options for Item formatting operations
static unsigned int itemOptions = 0;
typedef enum
{
  ITEM_DETAIL	= 0x00000001,	// -i: Display interpreted items    
  ITEM_HEAP		= 0x00000002,	// -x: Blocks contain heap items
  ITEM_INDEX	= 0x00000004,	// -y: Blocks contain heap items
  DEPARSE_HEAP	= 0x00000008,	// -p: deparse block
  ITEM_CHECKSUM	= 0x00000010	// -M: checksum for mirror files
}
itemSwitches;

// Options for Control File formatting operations
static unsigned int controlOptions = 0;
typedef enum
{
  CONTROL_DUMP = 0x00000001,	// -c: Dump control file
  CONTROL_FORMAT = BLOCK_FORMAT,	// -f: Formatted dump of control file
  CONTROL_FORCED = BLOCK_FORCED	// -S: Block size forced
}
controlSwitches;

// Possible value types for the Special Section
typedef enum
{
  SPEC_SECT_NONE,		// No special section on block
  SPEC_SECT_SEQUENCE,		// Sequence info in special section 
  SPEC_SECT_INDEX_BTREE,	// BTree index info in special section
  SPEC_SECT_INDEX_HASH,		// Hash index info in special section
  SPEC_SECT_INDEX_GIST,		// GIST index info in special section
  SPEC_SECT_INDEX_GIN,		// GIN index info in special section
  SPEC_SECT_ERROR_UNKNOWN,	// Unknown error 
  SPEC_SECT_ERROR_BOUNDARY	// Boundary error
}
specialSectionTypes;
static unsigned int specialType = SPEC_SECT_NONE;

// Possible return codes from option validation routine.
// pg_filedump doesn't do much with them now but maybe in
// the future...
typedef enum
{
  OPT_RC_VALID,			// All options are valid
  OPT_RC_INVALID,		// Improper option string
  OPT_RC_FILE,			// File problems
  OPT_RC_DUPLICATE,		// Duplicate option encountered
  OPT_RC_COPYRIGHT		// Copyright should be displayed
}
optionReturnCodes;

// Simple macro to check for duplicate options and then set
// an option flag for later consumption 
#define SET_OPTION(_x,_y,_z) if (_x & _y)               \
                               {                        \
                                 rc = OPT_RC_DUPLICATE; \
                                 duplicateSwitch = _z;  \
                               }                        \
                             else                       \
                               _x |= _y;

#define SEQUENCE_MAGIC 0x1717	// PostgreSQL defined magic number
#define EOF_ENCOUNTERED (-1)	// Indicator for partial read
#define BYTES_PER_LINE 16	// Format the binary 16 bytes per line

Datum
toast_flatten_tuple_attribute(Datum value,
	                          Oid typeId, int32 typeMod);

int Gp_segment = -2;
