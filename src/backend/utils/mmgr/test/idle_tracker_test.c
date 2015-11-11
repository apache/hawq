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
#include "miscadmin.h"

#include "../idle_tracker.c"

/*
 * This sets up an expected exception that will be rethrown for
 * verification using PG_TRY(), PG_CATCH() and PG_END_TRY() macros
 */
#define EXPECT_EXCEPTION()     \
	expect_any(ExceptionalCondition,conditionName); \
	expect_any(ExceptionalCondition,errorType); \
	expect_any(ExceptionalCondition,fileName); \
	expect_any(ExceptionalCondition,lineNumber); \
    will_be_called_with_sideeffect(ExceptionalCondition, &_ExceptionalCondition, NULL);\

/* MySessionState will use the address of this global variable */
static SessionState fakeSessionState;

/* Prepares a fake MySessionState pointer for use in the vmem tracker */
static void
InitFakeSessionState(int activeProcessCount, int cleanupCountdown, RunawayStatus runawayStatus, int pinCount, int vmem)
{
	MySessionState = &fakeSessionState;

	MySessionState->activeProcessCount = activeProcessCount;
	MySessionState->cleanupCountdown = cleanupCountdown;
	MySessionState->runawayStatus = runawayStatus;
	MySessionState->next = NULL;
	MySessionState->pinCount = pinCount;
	MySessionState->sessionId = 1234;
	MySessionState->sessionVmem = vmem;
	MySessionState->spinLock = 0;
}

/*
 * This method will emulate the real ExceptionalCondition
 * function by re-throwing the exception, essentially falling
 * back to the next available PG_CATCH();
 */
void
_ExceptionalCondition()
{
     PG_RE_THROW();
}

/*
 * A shared method to test proper activation during IdleTracker_Init()
 * or a direct call to IdleTracker_ActivateProcess() during regular
 * activation of an idle process.
 */
static void
CheckForActivation(void (*testFunc)(void))
{
	static EventVersion fakeCurrentVersion = 10;
	CurrentVersion = &fakeCurrentVersion;

	InitFakeSessionState(0 /* activeProcessCount */, CLEANUP_COUNTDOWN_BEFORE_RUNAWAY /* cleanupCountdown */,
			RunawayStatus_NotRunaway /* runawayStatus */, 1 /* pinCount */, 0 /* vmem */);

	EventVersion oldVersion = *CurrentVersion;

	activationVersion = 0;
	deactivationVersion = 0;

	assert_true(*CurrentVersion != activationVersion);
	assert_true(*CurrentVersion != deactivationVersion);

	/*
	 * Set to false as we want to verify that it gets set to true
	 * once the testFunc() call returns
	 */
	isProcessActive = false;

	assert_true(MySessionState->activeProcessCount == 0);

	testFunc();

	assert_true(activationVersion == *CurrentVersion);
	assert_true(deactivationVersion == 0);
	assert_true(isProcessActive == true);
	assert_true(MySessionState->activeProcessCount == 1);
}

/*
 * A shared method to test proper deactivation during IdleTracker_Shutdown()
 * or a direct call to IdleTracker_DeactivateProcess() during regular
 * deactivation of an active process when a proper cleanup was not possible
 * (e.g., a transaction is in progress).
 */
static void
CheckForDeactivationWithoutCleanup(void (*testFunc)(void))
{
	static EventVersion fakeCurrentVersion = 10;
	CurrentVersion = &fakeCurrentVersion;

	InitFakeSessionState(1 /* activeProcessCount */, CLEANUP_COUNTDOWN_BEFORE_RUNAWAY /* cleanupCountdown */,
			RunawayStatus_NotRunaway /* runawayStatus */, 1 /* pinCount */, 0 /* vmem */);

	EventVersion oldVersion = *CurrentVersion;
	/* Ensure we have a pending runaway event */
	EventVersion fakeLatestRunawayVersion = *CurrentVersion - 1;
	latestRunawayVersion = &fakeLatestRunawayVersion;

	activationVersion = 0;
	deactivationVersion = 0;

	assert_true(*CurrentVersion != activationVersion);
	assert_true(*CurrentVersion != deactivationVersion);

	/*
	 * Set to true as we want to verify that it gets set to false
	 * once the testFunc() call returns
	 */
	isProcessActive = true;

	assert_true(MySessionState->activeProcessCount == 1);

	/*
	 * Deactivation must call RunawayCleaner_StartCleanup before finishing deactivation
	 * to check for cleanup requirement for any pending runaway event. The method is
	 * supposed to throw an exception, but for this test we are mocking the function
	 * without side effect. I.e., the function behaves as if a proper cleanup is not
	 * possible
	 */
	will_be_called(RunawayCleaner_StartCleanup);

#ifdef USE_ASSERT_CHECKING
	will_return(RunawayCleaner_IsCleanupInProgress, true);
	/*
	 * Expecting an exception as we are indicating an ongoing cleanup
	 * and yet returning to IdleTracker_DactivateProcess
	 */
	EXPECT_EXCEPTION();
#endif

	PG_TRY();
	{
		testFunc();
#ifdef USE_ASSERT_CHECKING
		assert_false("Expected an assertion failure");
#endif
	}
	PG_CATCH();
	{

	}
	PG_END_TRY();

	assert_true(activationVersion == 0);
	assert_true(deactivationVersion == *CurrentVersion);
	assert_true(isProcessActive == false);
	assert_true(MySessionState->activeProcessCount == 0);
}

/*
 * A shared method to test proper deactivation during IdleTracker_Shutdown()
 * or a direct call to IdleTracker_DeactivateProcess() during regular
 * deactivation of an active process when a proper cleanup was done, throwing
 * and elog(ERROR,...) and therefore the call would never return to the calling
 * function
 */
static void
CheckForDeactivationWithProperCleanup(void (*testFunc)(void))
{
	static EventVersion fakeCurrentVersion = 10;
	CurrentVersion = &fakeCurrentVersion;

	InitFakeSessionState(1 /* activeProcessCount */, CLEANUP_COUNTDOWN_BEFORE_RUNAWAY /* cleanupCountdown */,
			RunawayStatus_NotRunaway /* runawayStatus */, 1 /* pinCount */, 0 /* vmem */);

	EventVersion oldVersion = *CurrentVersion;
	/* Ensure we have a pending runaway event */
	EventVersion fakeLatestRunawayVersion = *CurrentVersion - 1;
	latestRunawayVersion = &fakeLatestRunawayVersion;

	activationVersion = 0;
	deactivationVersion = 0;

	assert_true(*CurrentVersion != activationVersion);
	assert_true(*CurrentVersion != deactivationVersion);

	/*
	 * Set to true as we want to verify that it gets set to false
	 * once the testFunc() call returns
	 */
	isProcessActive = true;

	assert_true(MySessionState->activeProcessCount == 1);

	/*
	 * Deactivation must call RunawayCleaner_StartCleanup before finishing deactivation
	 * to check for cleanup requirement for any pending runaway event. The mocked method
	 * is throwing an exception here, which would block execution of code following the call
	 * site
	 */
	will_be_called_with_sideeffect(RunawayCleaner_StartCleanup, &_ExceptionalCondition, NULL);

	PG_TRY();
	{
		testFunc();
		assert_false("Expected an exception");
	}
	PG_CATCH();
	{

	}
	PG_END_TRY();

	assert_true(activationVersion == 0);
	assert_true(deactivationVersion == *CurrentVersion);
	assert_true(isProcessActive == false);
	assert_true(MySessionState->activeProcessCount == 0);
}

/*
 * A shared method to test that the IdleTracker_DeactivateProcess ignores
 * deactivation for an already idle process when the proc_exit_inprogress
 * is set to true
 */
static void
PreventDuplicateDeactivationDuringProcExit(void (*testFunc)(void))
{
	static EventVersion fakeCurrentVersion = 10;
	CurrentVersion = &fakeCurrentVersion;

	InitFakeSessionState(1 /* activeProcessCount */, CLEANUP_COUNTDOWN_BEFORE_RUNAWAY /* cleanupCountdown */,
			RunawayStatus_NotRunaway /* runawayStatus */, 1 /* pinCount */, 0 /* vmem */);

	EventVersion oldVersion = *CurrentVersion;
	/* Ensure we have a pending runaway event */
	EventVersion fakeLatestRunawayVersion = *CurrentVersion - 1;
	latestRunawayVersion = &fakeLatestRunawayVersion;

	activationVersion = 0;
	deactivationVersion = 0;

	assert_true(*CurrentVersion != activationVersion);
	assert_true(*CurrentVersion != deactivationVersion);

	/*
	 * Set to false to mark the process as deactivated
	 */
	isProcessActive = false;

	assert_true(MySessionState->activeProcessCount == 1);
	/*
	 * Setting proc_exit_inprogress to true means we won't try to
	 * deactivate an already idle process
	 */
	proc_exit_inprogress = true;
	/* Interrupts should be held off during proc_exit_inprogress*/
	InterruptHoldoffCount = 1;
	/* Now the deactivation should succeed as we have held off interrupts */
	testFunc();
	InterruptHoldoffCount = 0;

	assert_true(activationVersion == deactivationVersion);
	assert_true(deactivationVersion != *CurrentVersion);
	assert_true(isProcessActive == false);
	/* We haven't reduced the activeProcessCount */
	assert_true(MySessionState->activeProcessCount == 1);

#ifdef USE_ASSERT_CHECKING
	/*
	 * Now test that the testFunc fails assert if we try to deactivate an already
	 * idle process during a normal execution (i.e., not proc_exit_inprogress)
	 */
	proc_exit_inprogress = false;
	EXPECT_EXCEPTION();
	PG_TRY();
	{
		testFunc();
		assert_false("Expected assertion failure");
	}
	PG_CATCH();
	{

	}
	PG_END_TRY();
#endif
}

/*
 * Checks if IdleTracker_ShmemInit() properly initializes the global variables
 * as the postmaster
 */
void
test__IdleTracker_ShmemInit__InitializesGlobalVarsWhenPostmaster(void **state)
{
	IsUnderPostmaster = false;

	static EventVersion fakeCurrentVersion = 10;
	CurrentVersion = &fakeCurrentVersion;

	activationVersion = 0;
	deactivationVersion = 0;

	assert_true(*CurrentVersion != activationVersion);
	assert_true(*CurrentVersion != deactivationVersion);

	/*
	 * Set to true as we want to verfiy that it gets set to false
	 * once the IdleTracker_ShmemInit() call returns
	 */
	isProcessActive = true;

	IdleTracker_ShmemInit();

	assert_true(activationVersion == *CurrentVersion);
	assert_true(deactivationVersion == *CurrentVersion);
	assert_true(isProcessActive == false);
}

/*
 * Checks if IdleTracker_Init() activates the current process
 */
void
test__IdleTracker_Init__ActivatesProcess(void **state)
{
	will_return(VmemTracker_GetDynamicMemoryQuotaSema, 1024);
	CheckForActivation(&IdleTracker_Init);
}

/*
 * Checks if IdleTracker_ActivateProcess() activates the current process
 */
void
test__IdleTracker_ActivateProcess__ActivatesProcess(void **state)
{
	will_return(VmemTracker_GetDynamicMemoryQuotaSema, 1024);
	CheckForActivation(&IdleTracker_ActivateProcess);
}

/*
 * Checks if IdleTracker_DeactivateProcess() deactivates the current process
 * when a proper cleanup was done
 */
void
test__IdleTracker_DeactivateProcess__DeactivatesProcessWithCleanup(void **state)
{
	will_return(VmemTracker_GetDynamicMemoryQuotaSema, 1024);
	CheckForDeactivationWithProperCleanup(&IdleTracker_DeactivateProcess);
}

/*
 * Checks if IdleTracker_Shutdown() deactivates the current process when a
 * proper cleanup was done
 */
void
test__IdleTracker_Shutdown__DeactivatesProcessWithCleanup(void **state)
{
	will_return(VmemTracker_GetDynamicMemoryQuotaSema, 1024);
	CheckForDeactivationWithProperCleanup(&IdleTracker_Shutdown);
}

/*
 * Checks if IdleTracker_DeactivateProcess() deactivates the current process
 * when a proper cleanup could not be done
 */
void
test__IdleTracker_DeactivateProcess__DeactivatesProcessWithoutCleanup(void **state)
{
	will_return(VmemTracker_GetDynamicMemoryQuotaSema, 1024);
	CheckForDeactivationWithoutCleanup(&IdleTracker_DeactivateProcess);
}

/*
 * Checks if IdleTracker_Shutdown() deactivates the current process when a
 * proper cleanup could not be done
 */
void
test__IdleTracker_Shutdown__DeactivatesProcessWithoutCleanup(void **state)
{
	will_return(VmemTracker_GetDynamicMemoryQuotaSema, 1024);
	CheckForDeactivationWithoutCleanup(&IdleTracker_Shutdown);
}

/*
 * Checks if IdleTracker_DeactivateProcess() ignores deactivation if the
 * proc_exit_inprogress is set to true
 */
void
test__IdleTracker_DeactivateProcess__IgnoresDeactivationDuringProcExit(void **state)
{
	will_return_count(VmemTracker_GetDynamicMemoryQuotaSema, 1024, 2);
	PreventDuplicateDeactivationDuringProcExit(&IdleTracker_DeactivateProcess);
}

/*
 * Checks if IdleTracker_Shutdown() ignores deactivation if the
 * proc_exit_inprogress is set to true
 */
void
test__IdleTracker_Shutdown__IgnoresDeactivationDuringProcExit(void **state)
{
	will_return_count(VmemTracker_GetDynamicMemoryQuotaSema, 1024, 2);
	PreventDuplicateDeactivationDuringProcExit(&IdleTracker_Shutdown);
}

int
main(int argc, char* argv[])
{
        cmockery_parse_arguments(argc, argv);

        const UnitTest tests[] = {
        		unit_test(test__IdleTracker_ShmemInit__InitializesGlobalVarsWhenPostmaster),
				unit_test(test__IdleTracker_Init__ActivatesProcess),
				unit_test(test__IdleTracker_ActivateProcess__ActivatesProcess),
				unit_test(test__IdleTracker_DeactivateProcess__DeactivatesProcessWithCleanup),
				unit_test(test__IdleTracker_Shutdown__DeactivatesProcessWithCleanup),
				unit_test(test__IdleTracker_DeactivateProcess__DeactivatesProcessWithoutCleanup),
				unit_test(test__IdleTracker_Shutdown__DeactivatesProcessWithoutCleanup),
				/* Disable test temporarily due to feature changes, need to modify test accordingly later */
				#ifdef UNITTEST_DISABLE
				unit_test(test__IdleTracker_DeactivateProcess__IgnoresDeactivationDuringProcExit),
				unit_test(test__IdleTracker_Shutdown__IgnoresDeactivationDuringProcExit)
				#endif
        };
        return run_tests(tests);
}
