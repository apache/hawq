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
 * lockrmgr.h
 *
 * Lock subsystems two-phase state file hooks.
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCKRMGR_H_
#define LOCKRMGR_H_

#include "access/xlog.h"

extern void lock_recover_record(TransactionId xid, void *data, uint32 len);
extern void lock_postcommit(TransactionId xid, void *dummy1, uint32 dummy2);
extern void lock_postabort(TransactionId xid, void *dummy1, uint32 dummy2);

#endif   /* LOCKRMGR_H */
