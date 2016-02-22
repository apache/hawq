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

#ifndef ENVIRONMENT_SWITCH_H
#define ENVIRONMENT_SWITCH_H

#define BUILT_IN_HAWQ

#ifndef BUILT_IN_HAWQ

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <stddef.h>
#include <inttypes.h>

/* STL libs */
#include <unordered_map>
#include <list>
#include <vector>
#include <string>
#include <stack>
#include <algorithm>

#else

#include <math.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>			/* structure IN_ADDR */
#include <stdlib.h>
#include <pthread.h>
#include <sys/stat.h>
#include <signal.h>
#include <time.h>
#include <sys/poll.h>			/* poll facility. 							 */
#include <sys/types.h>
#include <sys/wait.h>

#include "c.h" 				    /* the definition of PGDDLIMPORT   */
#include "port.h"				/* get_progname() 							 */

#include "pg_config.h"			/* ENDIAN order. WORDS_BIGENDIAN */
#include "postgres.h"
#include "utils/palloc.h" 		/* Memory context and allocation functions.  */
#include "utils/guc.h"			/* GUC 										 */

#include "parser/parsetree.h"	/* Parse tree data structure. 				 */
#include "utils/memutils.h"		/* Memory context							 */
#include "libpq/libpq.h"	    /* Unix Domain Socket APIs.					 */
#include "libpq/pqcomm.h"		/* Unix Domain Socket APIs.					 */
#include "libpq/pqsignal.h"		/* Signal wrappers.							 */
#include "postmaster/syslogger.h"	/* Syslogger for independent mode.		 */
#include "postmaster/postmaster.h"
#include "cdb/cdbvars.h"		/* write_log() API.							 */
#include "pgtime.h"					/* Time zone for sys logger startup.	 */
#include "utils/ps_status.h"	/* ps display.								 */
#include "cdb/cdbutil.h"
#include "catalog/catquery.h"	/* catalog query ( caql ).					 */
#include "utils/builtins.h"		/* caql catalog table column types.			 */
#include "access/hash.h"
#endif

#include "errorcode.h"			/* Error code definitions.					 */
#include "utils/memutilities.h"	/* Memory context and manipulation wrapper   */

#define RMLOG rm_log_level

#ifndef UINT32_MAX
#define UINT32_MAX             (4294967295U)
#endif

#endif //ENVIRONMENT_SWITCH_H
