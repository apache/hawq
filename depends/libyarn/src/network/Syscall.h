/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_NETWORK_SYSCALL_H_
#define _HDFS_LIBHDFS3_NETWORK_SYSCALL_H_

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace System {

using ::recv;
using ::send;
using ::getaddrinfo;
using ::freeaddrinfo;
using ::socket;
using ::connect;
using ::getpeername;
using ::fcntl;
using ::setsockopt;
using ::poll;
using ::shutdown;
using ::close;

}

#ifdef MOCK

#include "MockSystem.h"
namespace HdfsSystem = MockSystem;

#else

namespace HdfsSystem = System;

#endif

#endif /* _HDFS_LIBHDFS3_NETWORK_SYSCALL_H_ */
