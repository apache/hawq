/*-------------------------------------------------------------------------
 *
 * optserver.h
 *	  Interface for Optimizer process management.
 *
 *
 * Copyright (c) 2012, EMC Corp.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef OPTSERVER_H
#define OPTSERVER_H

#include "postgres.h"

#ifdef __darwin__
#define OPT_LIB "libgpoptudf.dylib"
#else
#define OPT_LIB "libgpoptudf.so"
#endif

#define OPT_SOCKET_NAME "gp_opt_socket"
#define OPT_ERROR_BUFFER_SIZE	(4096)

/* struct with configuration parameters for GPOS task execution */
struct gpos_exec_params
{
	void *(*func)(void*);			/* task function */
	void *arg;						/* task argument */
	void *result;					/* task result */
	void *stack_start;				/* start of current thread's stack */
	char *error_buffer;				/* buffer used to store error messages */
	int error_buffer_size;			/* size of error message buffer */
	volatile bool *abort_requested;	/* flag indicating if abort is requested */
};


/* start optimizer */
extern int optimizer_start(void);

/* invoke optimizer function */
extern void *libopt_exec(const char *funcName, void *args, void *error_buffer,
		                 int error_buffer_size, volatile bool *abort);



#endif /* OPTSERVER_H */
