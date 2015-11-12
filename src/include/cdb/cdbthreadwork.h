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
 * cdbthreadwork.h
 *	  Manages a work thread.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBTHREADWORK_H
#define CDBTHREADWORK_H

#include "postgres.h"
#include <pthread.h>

typedef int (*ThreadWorkProc)(void *passThru, int command);

typedef enum ThreadWorkCommand
{
	No_WorkCommand = 0,
	Quit_WorkCommand,
	First_WorkCommand /* must always be last */
} ThreadWorkCommand;

typedef enum ThreadWorkAnswer
{
	No_WorkAnswer = 0,
	First_WorkAnswer /* must always be last */
} ThreadWorkAnswer;

typedef struct ThreadWork
{
	ThreadWorkProc  	workProc;
	void				*passThru;
	
	pthread_mutex_t 	mutex;
	pthread_cond_t 		commandCond;
	pthread_cond_t 		answerCond;
	
	int					command;
	int					answer;

	bool				waitingForCommand;
	bool				waitingForAnswer;
	
	pthread_t			thread;
} ThreadWork;

/*
 * Start the work thread.
 */
void ThreadWorkStart(
    ThreadWork         *threadWork,
    ThreadWorkProc     workProc,
    void			   *passThru);

/*
 * Give command to the work thread.
 */
extern void ThreadWorkGiveCommand(
    ThreadWork          *threadWork,
    int  			    command);

/*
 * Get answer from the work thread.
 */
extern void ThreadWorkGetAnswer(
    ThreadWork         *threadWork,
    int  			   *answer);

/*
 * Try (i.e. poll) to get answer from the work thread.
 */
extern bool ThreadWorkTryAnswer(
    ThreadWork         *threadWork,
    int  			   *answer);

/*
 * Give quit to the work thread and wait for it to finish.
 */
extern void ThreadWorkQuit(
    ThreadWork         *threadWork);

#endif   /* CDBTHREADWORK_H */
