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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * testframe.h
 *     This file contains some simple unit-testing framework that is shared
 *     across all of the unit-tests.
 *
 * This is borrowed from donnie's DTM testing.  eventually it should be 
 * either something different or something that is more shareable with
 * other unit tests.
 *
 *-------------------------------------------------------------------------
 */
#ifndef TESTFRAME_H
#define TESTFRAME_H


#define RUN_TEST(testfunc)  \
  printf("[TEST-START ] %s\n", #testfunc);  \
  testfunc();  \
  printf("[TEST-FINISH] %s\n", #testfunc)


#define VERIFY(expr)  \
  ((void) ((expr) ? TEST_PASS(#expr) : TEST_FAIL(#expr)))

#define VERIFY_INIT(expr)  \
  if (expr) { TEST_PASS(#expr); } else { TEST_FAIL(#expr); goto Error; }


#define TEST_PASS(expr)  \
  (printf("[PASS] %s:%u:  '%s'\n", __FILE__, __LINE__, expr), 0)

#define TEST_FAIL(expr)  \
  (printf("[FAIL] %s:%u:  '%s'\n", __FILE__, __LINE__, expr), 0)


#endif /* TESTFRAME_H */
