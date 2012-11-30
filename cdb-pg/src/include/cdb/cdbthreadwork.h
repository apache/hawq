/*-------------------------------------------------------------------------
 *
 * cdbthreadwork.h
 *	  Manages a work thread.
 *
 * Copyright (c) 2007, greenplum inc
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
