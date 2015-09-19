/*-------------------------------------------------------------------------
 *
 * queue.h
 *	  Commands for manipulating resource queues.
 *
 * Copyright (c) 2006-2010, Greenplum inc.
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef QUEUE_H
#define QUEUE_H

extern List *
GetResqueueCapabilityEntry(Oid  queueid);

extern char *GetResqueueName(Oid resqueueOid);
#endif   /* QUEUE_H */
