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
