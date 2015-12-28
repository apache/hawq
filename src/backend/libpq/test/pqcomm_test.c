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
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "postgres.h"
#include "nodes/nodes.h"
#include "libpq/libpq.h"
#include "../pqcomm.c"

/* Number of bytes requested to be sent through internal_flush */ 
#define TEST_NUM_BYTES 100

/*
 *  Test for internal_flush() for the case when:
 *    - requesting to send TEST_NUM_BYTES bytes
 *    - secure_write returns TEST_NUM_BYTES (send successful)
 *    - errno is not changed
 */
void
test__internal_flush_succesfulSend(void **state)
{

  expect_any(secure_write, port);
  expect_any(secure_write, ptr); 
  expect_any(secure_write, len); 
  will_return(secure_write, TEST_NUM_BYTES);
  
  PqSendPointer = TEST_NUM_BYTES; 
  int result = internal_flush();

  assert_int_equal(result,0);
  assert_int_equal(ClientConnectionLost, 0);
  assert_int_equal(InterruptPending, 0);
}

/*
 * Simulate side effects of secure_write. Sets the errno variable to val
 */
void
_set_errno(void *val)
{
  errno = * ((int *) val); 
}

/*
 *  Test for internal_flush() for the case when:
 *    - secure_write returns 0 (send failed)
 *    - errno is set to EINTR
 */
void
test__internal_flush_failedSendEINTR(void **state)
{

	/*
	 * In the case secure_write gets interrupted, we loop around and
	 * try the send again.
	 * In this test we simulate that, and secure_write will be called twice.
	 *
	 * First call to secure_write: returns 0 and sets errno to EINTR.
	 */
	expect_any(secure_write, port);
	expect_any(secure_write, ptr);
	expect_any(secure_write, len);
	static int errval = EINTR;
	will_return_with_sideeffect(secure_write, 0, _set_errno, &errval);

	/* Second call to secure_write: returns TEST_NUM_BYTES, i.e. success */
	expect_any(secure_write, port);
	expect_any(secure_write, ptr);
	expect_any(secure_write, len);
	will_return(secure_write, TEST_NUM_BYTES);

	PqSendPointer = TEST_NUM_BYTES;

	/* Call function under test */
	int result = internal_flush();

	assert_int_equal(result,0);
	assert_int_equal(ClientConnectionLost, 0);
	assert_int_equal(InterruptPending, 0);
}

/*
 *  Test for internal_flush() for the case when:
 *    - secure_write returns 0 (send failed)
 *    - errno is set to EPIPE
 */
void
test__internal_flush_failedSendEPIPE(void **state)
{

	/* Simulating that secure_write will fail, and set the errno to EPIPE */
	expect_any(secure_write, port);
	expect_any(secure_write, ptr);
	expect_any(secure_write, len);
	static int errval = EPIPE;
	will_return_with_sideeffect(secure_write, 0, _set_errno, &errval);

	/* In that case, we expect ereport(COMERROR, ...) to be called */
	expect_value(errstart, elevel, COMMERROR);
	expect_any(errstart, filename);
	expect_any(errstart, lineno);
	expect_any(errstart, funcname);
	expect_any(errstart, domain);
	will_return(errstart, false);

	PqSendPointer = TEST_NUM_BYTES;

	/* Call function under test */
	int result = internal_flush();

	assert_int_equal(result,EOF);
	assert_int_equal(ClientConnectionLost, 1);
	assert_int_equal(InterruptPending, 1);

}

/* ==================== main ==================== */
int
main(int argc, char* argv[])
{
    cmockery_parse_arguments(argc, argv);

    const UnitTest tests[] = 
      {
	unit_test(test__internal_flush_succesfulSend),
	unit_test(test__internal_flush_failedSendEINTR),
	unit_test(test__internal_flush_failedSendEPIPE)
      };

    return run_tests(tests);
}
