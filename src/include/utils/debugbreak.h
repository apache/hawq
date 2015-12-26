/*
 * debugbreak.h
 * 		Debugging facilities
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

#ifndef _DEBUG_BREAK_H_
#define _DEBUG_BREAK_H_

#define DEBUG_BREAK_POINT_ASSERT 		(1)
#define DEBUG_BREAK_POINT_EXCEPTION 	(1 << 1)

#ifdef USE_DEBUG_BREAK
extern void debug_break(void);
extern void debug_break_n(int n);
extern void enable_debug_break_n(int n);
extern void disable_debug_break_n(int n);
extern void debug_break_timed(int sec, bool singleton);

#else
#define debug_break()
#define debug_break_n(n)
#define enable_debug_break_n(n)
#define disable_debug_break_n(n)
#define debug_break_timed(sec, singleton);
#endif

#endif /* _DEBUG_BREAK_H_ */


