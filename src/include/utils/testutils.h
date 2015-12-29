/*
 * testutils.h
 *		Collection of testing utilities.
 *
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
 *
 */

#ifndef TESTUTILS_H_
#define TESTUTILS_H_

#ifdef USE_TEST_UTILS

#define CHECK_TIME_SLICE()      TimeSliceCheck(__FILE__, __LINE__)

/* external variables */
extern PGDLLIMPORT volatile int32 InterruptHoldoffCount;
extern PGDLLIMPORT volatile int32 CritSectionCount;

/* time slicing */
extern void TimeSliceReset(void);
extern void TimeSliceCheck(const char *file, int line);

#if defined(__i386) || defined(__x86_64__)

#define USE_TEST_UTILS_X86   1

/* detect database-lightweight lock conflict */
extern void LWLockHeldDetect(const void *locktag, int lockmode);

#endif /* defined(__i386) || defined(__x86_64__) */

/* GUCs */
extern bool gp_test_time_slice;          /* session GUC, controls time slice violation checking */
extern int  gp_test_time_slice_interval; /* session GUC, sets time slice interval in ms */
extern int  gp_test_time_slice_report_level;
                                         /* session GUC, sets level of violation report messages */
extern bool gp_test_deadlock_hazard;     /* session GUC, controls database-lightweight lock conflict detection */
extern int  gp_test_deadlock_hazard_report_level;
                                         /* session GUC, sets level of hazard report messages */

#endif /* USE_TEST_UTILS */

#endif /* TESTUTILS_H_ */


/* EOF */
