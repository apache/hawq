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

/*-------------------------------------------------------------------------
 *
 * cdbtimer.h
 *	  Functions to manipulate timers used in a backend.
 *
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBTIMER_H_
#define CDBTIMER_H_

#include <sys/time.h>

typedef struct itimers {
	struct itimerval rtimer;		/* ITIMER_REAL */
	struct itimerval vtimer;		/* ITIMER_VIRTUAL */
	struct itimerval ptimer;		/* ITIMER_PROF */
} itimers;

void resetTimers(struct itimers *timers);
void restoreTimers(struct itimers *timers);

#endif /* CDBTIMER_H_ */
