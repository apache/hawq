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
/* 
 * faultinject.c
 * 	 Fault injection utilities
 * 
 */

#include "postgres.h"
#include "funcapi.h"
#include "utils/faultinjection.h"
#include "utils/debugbreak.h"

#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "storage/bfz.h"
#include "storage/buffile.h"

#include "postmaster/syslogger.h"
#include "utils/vmem_tracker.h"

#include <unistd.h>

/* exposed in pg_proc.h */
extern Datum gp_fault_inject(PG_FUNCTION_ARGS) ;
Datum gp_fault_inject(PG_FUNCTION_ARGS) 
{
#ifdef USE_TEST_UTILS
	int64 reason = PG_GETARG_INT32(0);
	int64 arg = PG_GETARG_INT64(1);

	int ret = gp_fault_inject_impl(reason, arg); 
	PG_RETURN_INT64(ret);
#else
	PG_RETURN_INT64(0);
#endif
}

#ifdef USE_TEST_UTILS

static char * longmsg = 
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
"12345678901234567890123456789012345678901234567890"
;

static char *multiline = "1234567890\n1234567890\n";
static char *quotestr = "\"\"\nXXX\nYYY\"\"\n\r\n\"\tZZZ\t\"\"\\\\\"";

static void open_many_files(int file_type);

bool gp_fault_inject_segment_failure = false;
int gp_fault_inject_segment_failure_segment_id = -1;

int64
gp_fault_inject_impl(int32 reason, int64 arg)
{
	switch(reason)
	{
		case GP_FAULT_USER_SEGV:
			*(int *) 0 = 1234;
			break;
		case GP_FAULT_USER_LEAK:
			palloc(arg);
			break;
		case GP_FAULT_USER_LEAK_TOP:
			{
				extern MemoryContext TopMemoryContext;
				MemoryContext oldCtxt = MemoryContextSwitchTo(TopMemoryContext);
				palloc(arg);
				MemoryContextSwitchTo(oldCtxt);
			}
			break;
		case GP_FAULT_USER_RAISE_ERROR:
			elog(ERROR, "User fault injection raised error");
		case GP_FAULT_USER_RAISE_FATAL:
			elog(FATAL, "User fault injection raised fatal");
		case GP_FAULT_USER_RAISE_PANIC:
			elog(PANIC, "User fault injection raised panic");
		case GP_FAULT_USER_PROCEXIT:
			{
				extern void proc_exit(int);
				proc_exit((int) arg);
			}
		case GP_FAULT_USER_ABORT:
			abort();

		case GP_FAULT_USER_INFINITE_LOOP:
			{
				do {
					/* do nothing */
				} while(true);
			}
			break;
		case GP_FAULT_USER_ASSERT_FAILURE:
			Assert(!"Inject an assert failure");
			break;

        case GP_FAULT_USER_DEBUGBREAK:
            {
                if (arg == 0)
                    debug_break();
                else
                    debug_break_n(arg);
            }
            break;

		case GP_FAULT_USER_SEGV_CRITICAL:
			START_CRIT_SECTION();
			*(int *) 0 = 1234;
			END_CRIT_SECTION();
			break;

		case GP_FAULT_USER_SEGV_LWLOCK:
			LWLockAcquire(WALInsertLock, LW_EXCLUSIVE);
			*(int *) 0 = 1234;
			break;
 	
		case GP_FAULT_USER_OPEN_MANY_FILES:
			open_many_files((int) arg);
			break;

		case GP_FAULT_USER_MP_CONFIG:
		case GP_FAULT_USER_MP_ALLOC:
		case GP_FAULT_USER_MP_HIGHWM:
		case GP_FAULT_SEG_AVAILABLE:
		case GP_FAULT_SEG_SET_VMEMMAX:
		case GP_FAULT_SEG_GET_VMEMMAX:
			return VmemTracker_Fault(reason, arg);

                case GP_FAULT_LOG_LONGMSG:
                        elog(LOG, 
                                        "%s%s%s%s%s" "%s%s%s%s" "%s%s%s%s%s", 
                                        longmsg, longmsg, longmsg, longmsg, longmsg,
                                        multiline, multiline, quotestr, quotestr,
                                        longmsg, longmsg, longmsg, longmsg, longmsg
                            );
                        break;
                case GP_FAULT_LOG_3RDPARTY:
                        fprintf(stderr, "Hello from 3rd party");
						fflush(stderr);
                        break;

                case GP_FAULT_LOG_3RDPARTY_LONGMSG:
                        fprintf(stderr,  
                                        "%s%s%s%s%s" "%s%s%s%s" "%s%s%s%s%s", 
                                        longmsg, longmsg, longmsg, longmsg, longmsg,
                                        multiline, multiline, quotestr, quotestr,
                                        longmsg, longmsg, longmsg, longmsg, longmsg
                               );
			fflush(stderr);
                        break;
                        
                case GP_FAULT_LOG_CRASH:
                        {
                                PipeProtoHeader hdr;
                                hdr.zero = 0;
                                hdr.len = 0;
                                hdr.pid = 1; 
                                hdr.thid = 1; 
                                hdr.main_thid = 1;
                                hdr.chunk_no = 0;
                                hdr.is_last = 't';
                                hdr.log_format = 'X';
                                hdr.log_line_number = reason;
                                hdr.next = -1;
                                write(2, &hdr, sizeof(PipeProtoHeader));
                         }
                         break;

		case GP_FAULT_INJECT_SEGMENT_FAILURE:
			{
				gp_fault_inject_segment_failure = true;
				gp_fault_inject_segment_failure_segment_id = (int)arg;
				elog(LOG, "Inject a segment failure for segment %d", (int)arg);
			}
			break;

		default:
			elog(ERROR, "Invalid user fault injection code");
	}
	return 0; 
}

/*
 * Open many bfz files to simulate running out of file handles.
 * file_type values:
 *   0 - bfz, no compression
 *   1 - bfz, zlib compression
 *   2 - buffile
 */
static void
open_many_files(int file_type)
{
	char file_name[MAXPGPATH];
	int iter = 0;

	while (true)
	{
		CHECK_FOR_INTERRUPTS();
		snprintf(file_name, MAXPGPATH, "fake_file_%d", iter);
		switch(file_type)
		{
		case 0:
		case 1:
			;
#if USE_ASSERT_CHECKING
			bfz_t *bfz_file =
#endif
			bfz_create(file_name, true /* delOnClose */, file_type);
			Assert(NULL != bfz_file);
			break;

		case 2:
			;
#if USE_ASSERT_CHECKING
			BufFile *buf_file =
#endif
			BufFileCreateTemp(file_name, false /* interXact */ );
			Assert(NULL != buf_file);
			break;

		default:
			Assert(false && "argument for fault type not supported");
		}

		iter++;
	}

	return;
}

#endif

	
