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
 * cdbbufferedread.c
 *	  Manages a work thread.
 *
 * (See .h file for usage comments)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <pthread.h>
#include <limits.h>
#include "cdb/cdbthreadwork.h"
#include "cdb/cdbvars.h"

#ifndef _WIN32
#define mythread() ((unsigned long) pthread_self())
#else
#define mythread() ((unsigned long) pthread_self().p)
#endif

static void ThreadWork_Main_create_thread(
    ThreadWork         *threadWork);
static void ThreadWork_Process_pthread_join(
    ThreadWork         *threadWork);
static void ThreadWork_mutex_lock(
    ThreadWork         *threadWork,
    pthread_mutex_t    *mutex,
    bool               isProcess);
static void ThreadWork_mutex_unlock(
    ThreadWork         *threadWork,
    pthread_mutex_t    *mutex,
    bool               isProcess);
static void ThreadWork_cond_signal(
    ThreadWork         *threadWork,
    pthread_cond_t     *cond,
    bool               isProcess);
static void ThreadWork_cond_wait(
    ThreadWork         *threadWork,
    pthread_cond_t     *cond,
    pthread_mutex_t    *mutex,
    bool               isProcess);
/*
static bool ThreadWork_cond_timedwait(
    ThreadWork         *threadWork,
    pthread_cond_t     *cond,
    pthread_mutex_t    *mutex,
	struct timespec    *timeout,
    bool               isProcess);
*/
static void* ThreadWork_Thread(void *arg);


static void ThreadWork_Main_create_thread(
    ThreadWork         *threadWork)
{
	int pthread_err = 0;
	pthread_attr_t t_atts;

	Assert(threadWork != NULL);
	
	/* save ourselves some memory: the defaults for thread stack
	 * size are large (1M+) */
	pthread_err = pthread_attr_init(&t_atts);
	if (pthread_err != 0)
	{
//		elog(ERROR, "ThreadWork: pthread_attr_init failed with error %d", pthread_err);
		fprintf(stderr, "ThreadWork: pthread_attr_init failed with error %d", pthread_err);
		exit(1);	// UNDONE
	}

#ifdef pg_on_solaris
	/* Solaris doesn't have PTHREAD_STACK_MIN ? */
	pthread_err = pthread_attr_setstacksize(&t_atts, (256*1024));
#else
	pthread_err = pthread_attr_setstacksize(&t_atts, Max(PTHREAD_STACK_MIN, (256*1024)));
#endif
	if (pthread_err != 0)
	{
//		elog(ERROR, "ThreadWork: pthread_attr_setstacksize failed with error %d", pthread_err);
		fprintf(stderr, "ThreadWork: pthread_attr_setstacksize failed with error %d", pthread_err);
		pthread_attr_destroy(&t_atts);
		exit(1);	// UNDONE
	}

	pthread_err = pthread_create(&threadWork->thread, &t_atts, ThreadWork_Thread, threadWork);
	if (pthread_err != 0)
	{
//		elog(ERROR, "ThreadWork: pthread_create failed with error %d", pthread_err);
		fprintf(stderr, "ThreadWork: pthread_create failed with error %d", pthread_err);
		pthread_attr_destroy(&t_atts);
		exit(1);	// UNDONE
	}

	pthread_attr_destroy(&t_atts);
}

/*
 * Start the work thread.
 */
void ThreadWorkStart(
    ThreadWork         *threadWork,
    ThreadWorkProc     workProc,
    void			   *passThru)
{
	Assert(threadWork != NULL);
	Assert(workProc != NULL);

	memset(threadWork, 0, sizeof(ThreadWork));
	threadWork->workProc = workProc;
	threadWork->passThru = passThru;
	
	pthread_mutex_init(&threadWork->mutex, NULL);
	pthread_cond_init(&threadWork->commandCond, NULL);
	pthread_cond_init(&threadWork->answerCond, NULL);
	
	threadWork->command = No_WorkCommand;
	threadWork->answer = No_WorkAnswer;

	threadWork->waitingForCommand = false;
	threadWork->waitingForAnswer = false;
	
	ThreadWork_Main_create_thread(threadWork);
}

static void ThreadWork_Process_pthread_join(
    ThreadWork         *threadWork)
{
	int pthread_err;
	
	Assert(threadWork != NULL);
	
	pthread_err = pthread_join(threadWork->thread, NULL);
	if (pthread_err != 0)
	{
//		elog(ERROR, "ThreadWork: pthread_join failed on thread %lu returned %d attempting to join to %lu)",
		fprintf(stderr, "ThreadWork: pthread_join failed on thread %lu returned %d attempting to join to %lu)",
#ifndef _WIN32
			 (unsigned long) threadWork->thread, 
#else
			 (unsigned long) threadWork->thread.p,
#endif
			 pthread_err, (unsigned long)mythread());
		exit(1);	// UNDONE
	}
}

static void ThreadWork_mutex_lock(
    ThreadWork         *threadWork,
    pthread_mutex_t    *mutex,
    bool               isProcess)
{
	int pthread_err;
	
	Assert(threadWork != NULL);
	
	pthread_err = pthread_mutex_lock(mutex);
	if (pthread_err != 0)
	{
//		if (isProcess)
//			elog(ERROR,"ThreadWork: pthread_mutex_lock failed with error %d",
//				 pthread_err);
//		else
//		{
//			write_log("ThreadWork: pthread_mutex_lock failed with error %d",
//				      pthread_err);
			fprintf(stderr, "ThreadWork: pthread_mutex_lock failed with error %d",
				      pthread_err);
			exit(1);	// UNDONE.
//		}
	}
}

static void ThreadWork_mutex_unlock(
    ThreadWork         *threadWork,
    pthread_mutex_t    *mutex,
    bool               isProcess)
{
	int pthread_err;
	
	Assert(threadWork != NULL);
	
	pthread_err = pthread_mutex_unlock(mutex);
	if (pthread_err != 0)
	{
//		if (isProcess)
//			elog(ERROR,"ThreadWork: pthread_mutex_unlock failed with error %d",
//				 pthread_err);
//		else
//		{
//			write_log("ThreadWork: pthread_mutex_unlock failed with error %d",
//				      pthread_err);
			fprintf(stderr,"ThreadWork: pthread_mutex_unlock failed with error %d",
				      pthread_err);
			exit(1);	// UNDONE.
//		}
	}
}

static void ThreadWork_cond_signal(
    ThreadWork         *threadWork,
    pthread_cond_t     *cond,
    bool               isProcess)
{
	int pthread_err;
	
	Assert(threadWork != NULL);
	
	pthread_err = pthread_cond_signal(cond);
	if (pthread_err != 0)
	{
//		if (isProcess)
//			elog(ERROR,"ThreadWork: pthread_cond_signal failed with error %d",
//				 pthread_err);
//		else
//		{
//			write_log("ThreadWork: pthread_cond_signal failed with error %d",
//				      pthread_err);
			fprintf(stderr,"ThreadWork: pthread_cond_signal failed with error %d",
				      pthread_err);
			exit(1);	// UNDONE.
//		}
	}
}

static void ThreadWork_cond_wait(
    ThreadWork         *threadWork,
    pthread_cond_t     *cond,
    pthread_mutex_t    *mutex,
    bool               isProcess)
{
	int pthread_err;
	
	Assert(threadWork != NULL);
	Assert(cond != NULL);
	
	pthread_err = pthread_cond_wait(
								cond,
								mutex);
	if (pthread_err != 0)
	{
//		if (isProcess)
//			elog(ERROR,"ThreadWork: pthread_cond_wait failed with error %d",
//				 pthread_err);
//		else
//		{
//			write_log("ThreadWork: pthread_cond_wait failed with error %d",
//				      pthread_err);
			fprintf(stderr,"ThreadWork: pthread_cond_wait failed with error %d",
				      pthread_err);
			exit(1);	// UNDONE.
//		}
	}
}

/*
static bool ThreadWork_cond_timedwait(
    ThreadWork         *threadWork,
    pthread_cond_t     *cond,
    pthread_mutex_t    *mutex,
	struct timespec    *timeout,
    bool               isProcess)
{
	int pthread_err;
	
	Assert(threadWork != NULL);
	Assert(cond != NULL);
	Assert(timeout != NULL);
	
	pthread_err = pthread_cond_timedwait(
									cond,
									mutex,
									timeout);
	if (pthread_err != 0)
	{
		if (pthread_err == ETIMEDOUT)
			return false;
		
//		if (isProcess)
//			elog(ERROR,"ThreadWork: pthread_cond_wait failed with error %d",
//				 pthread_err);
//		else
//		{
//			write_log("ThreadWork: pthread_cond_wait failed with error %d",
//				      pthread_err);
			fprintf(stderr,"ThreadWork: pthread_cond_wait failed with error %d",
				      pthread_err);
			exit(1);	// UNDONE.
//		}
	}

	return true;
}
*/

/*
 * Give command to the work thread.
 */
void ThreadWorkGiveCommand(
    ThreadWork          *threadWork,
    int  				command)
{	
	Assert(threadWork != NULL);
	
	ThreadWork_mutex_lock(
					threadWork,
					&threadWork->mutex,
					/* isProcess */ true);
	
	Assert(threadWork->command == No_WorkCommand);
	Assert(threadWork->answer == No_WorkAnswer);
	threadWork->command = command;

	if (threadWork->waitingForCommand)
		ThreadWork_cond_signal(
						threadWork,
						&threadWork->commandCond,
						/* isProcess */ true);
	
	ThreadWork_mutex_unlock(
            		threadWork,
					&threadWork->mutex,
					/* isProcess */ true);
}

/*
 * Get answer from the work thread.
 */
void ThreadWorkGetAnswer(
    ThreadWork         *threadWork,
    int  			   *answer)
{	
	Assert(threadWork != NULL);
	Assert(answer != NULL);

	ThreadWork_mutex_lock(
					threadWork,
					&threadWork->mutex,
					/* isProcess */ true);
	
	while (threadWork->answer == No_WorkAnswer)
	{
		/*
		 * The condition is we are waiting for is an answer.
		 */
		threadWork->waitingForAnswer = true;
		ThreadWork_cond_wait(
						threadWork,
						&threadWork->answerCond,
						&threadWork->mutex,
						/* isProcess */ false);
		threadWork->waitingForAnswer = false;
	}
	
	*answer = threadWork->answer;
	
	/*
	 * Clear for next command.
	 */
	threadWork->answer = No_WorkAnswer;
	
	ThreadWork_mutex_unlock(
            		threadWork,
					&threadWork->mutex,
					/* isProcess */ true);
}

/*
 * Try (i.e. poll) to get answer from the work thread.
 */
bool ThreadWorkTryAnswer(
    ThreadWork         *threadWork,
    int  			   *answer)
{	
	Assert(threadWork != NULL);
	Assert(answer != NULL);

	ThreadWork_mutex_lock(
					threadWork,
					&threadWork->mutex,
					/* isProcess */ true);

	*answer = threadWork->answer;
	if (*answer != No_WorkAnswer)
	{
		/*
		 * Clear for next command.
		 */
		threadWork->answer = No_WorkAnswer;
	}
	
	ThreadWork_mutex_unlock(
            		threadWork,
					&threadWork->mutex,
					/* isProcess */ true);


	return (*answer != No_WorkAnswer);
}


/*
 * Give quit to the work thread and wait for it to finish.
 */
void ThreadWorkQuit(
    ThreadWork         *threadWork)
{	
	Assert(threadWork != NULL);

	ThreadWorkGiveCommand(
					threadWork,
					Quit_WorkCommand);

	/*
	 * Wait for thread to finish.
	 */
	ThreadWork_Process_pthread_join(threadWork);

	pthread_mutex_destroy(&threadWork->mutex);
	pthread_cond_destroy(&threadWork->commandCond);
	pthread_cond_destroy(&threadWork->answerCond);
}

static void* ThreadWork_Thread(void *arg)
{
	ThreadWork *threadWork = (ThreadWork*)arg;
	
	int command;
	int answer;
	
	while (true)
	{
		ThreadWork_mutex_lock(
						threadWork,
						&threadWork->mutex,
						/* isProcess */ false);

		while (threadWork->command == No_WorkCommand)
		{
			/*
			 * The condition is we are waiting for is a command.
			 */
			threadWork->waitingForCommand = true;
			ThreadWork_cond_wait(
							threadWork,
							&threadWork->commandCond,
							&threadWork->mutex,
							/* isProcess */ false);
			threadWork->waitingForCommand = false;
		}
		
		command = threadWork->command;
		
		ThreadWork_mutex_unlock(
						threadWork,
						&threadWork->mutex,
						/* isProcess */ false);

		if (command == Quit_WorkCommand)
			break;

		answer = threadWork->workProc(threadWork->passThru, command);
		Assert(answer != No_WorkAnswer);
		
		/*
		 * Give answer to main thread.
		 */
		ThreadWork_mutex_lock(
						threadWork,
						&threadWork->mutex,
						/* isProcess */ false);
		
		Assert(threadWork->command == command);
		threadWork->command = No_WorkCommand;
		
		Assert(threadWork->answer == No_WorkAnswer);
		threadWork->answer = answer;

		if (threadWork->waitingForAnswer)
			ThreadWork_cond_signal(
							threadWork,
							&threadWork->answerCond,
							/* isProcess */ false);

		ThreadWork_mutex_unlock(
						threadWork,
						&threadWork->mutex,
						/* isProcess */ false);
	}

	return NULL;
}

