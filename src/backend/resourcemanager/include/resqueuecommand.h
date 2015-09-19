#ifndef DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_QUEUE_COMMAND_H
#define DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_QUEUE_COMMAND_H

void createResourceQueue(CreateQueueStmt *stmt);
void dropResourceQueue(DropQueueStmt *stmt);
void alterResourceQueue(AlterQueueStmt *stmt);
//extern void AlterQueue(AlterQueueStmt *stmt);
//extern void DropQueue(DropQueueStmt *stmt);
//extern char *GetResqueueName(Oid resqueueOid);
//extern char *GetResqueuePriority(Oid queueId);

#endif /* DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_QUEUE_COMMAND_H */
