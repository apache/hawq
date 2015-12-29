/*
 * atomic.h
 *    Header file for atomic operations.
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
extern int32 compare_and_swap_32(uint32 *dest, uint32 old, uint32 new);
extern int32 compare_and_swap_64(uint64 *dest, uint64 old, uint64 new);
extern int32 compare_and_swap_ulong(unsigned long *dest,
									unsigned long oldval,
									unsigned long newval);
extern int32 gp_lock_test_and_set(volatile int32 *ptr, int32 val);
extern int32 gp_atomic_add_32(volatile int32 *ptr, int32 inc);
extern int64 gp_atomic_add_int64(int64 *ptr, int64 inc);
extern uint64 gp_atomic_add_uint64(uint64 *ptr, int64 inc);


extern int32 gp_atomic_incmod_32(volatile int32 *loc, int32 mod);
extern uint32 gp_atomic_dec_positive_32(volatile uint32 *loc, uint32 dec);
extern uint32 gp_atomic_inc_ceiling_32(volatile uint32 *loc, uint32 inc, uint32 ceil);
