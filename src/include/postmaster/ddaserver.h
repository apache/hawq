/*-------------------------------------------------------------------------
 *
 * ddaserver.h
 *	  Under the QD postmaster the ddaserver combines lock information 
 *    from QEs to detect deadlocks.  Each QE has a local ddaserver that
 *    communicates with the master ddaserver at the QD when it finds
 *    a lock waiter.
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
 *-------------------------------------------------------------------------
 */
#ifndef DDASERVER_H
#define DDASERVER_H

#ifndef _WIN32
#include <stdint.h>             /* uint32_t (C99) */
#else
typedef unsigned int uint32_t;
#endif 


extern void DdaserverShmemInit(void);
extern int DdaserverShmemSize(void);
extern int ddaserver_start(void);





#endif   /* DDASERVER_H */
