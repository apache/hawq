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
#define NOFIXADE
#define NEED_STRDUP

#ifndef			BIG_ENDIAN
#define			BIG_ENDIAN		4321
#endif
#ifndef			LITTLE_ENDIAN
#define			LITTLE_ENDIAN	1234
#endif
#ifndef			PDP_ENDIAN
#define			PDP_ENDIAN		3412
#endif
#ifndef			BYTE_ORDER
#define			BYTE_ORDER		LITTLE_ENDIAN
#endif

/*
 * Except for those system calls and library functions that are either
 * - covered by the C standard library and Posix.1
 * - or need a declaration to declare parameter or return types,
 * most Ultrix 4 calls are not declared in the system header files.
 * The rest of this header is used to remedy this for PostgreSQL to give a
 * warning-free compilation.
 */

#include <sys/types.h>			/* Declare various types, e.g. size_t, fd_set */

extern int	fp_class_d(double);
extern long random(void);

struct rusage;
extern int	getrusage(int, struct rusage *);

extern int	ioctl(int, unsigned long,...);

extern int	socket(int, int, int);
struct sockaddr;
extern int	connect(int, const struct sockaddr *, int);
typedef int ssize_t;
extern ssize_t send(int, const void *, size_t, int);
extern ssize_t recv(int, void *, size_t, int);
extern int	setsockopt(int, int, int, const void *, int);
extern int	bind(int, const struct sockaddr *, int);
extern int	listen(int, int);
extern int	accept(int, struct sockaddr *, int *);
extern int	getsockname(int, struct sockaddr *, int *);
extern ssize_t recvfrom(int, void *, size_t, int, struct sockaddr *, int *);
extern ssize_t sendto(int, const void *, size_t, int, const struct sockaddr *, int);
struct timeval;
extern int	select(int, fd_set *, fd_set *, fd_set *, struct timeval *);

extern int	gethostname(char *, int);

extern int	getopt(int, char *const *, const char *);
extern int	putenv(const char *);

struct itimerval;
extern int	setitimer(int, const struct itimerval *, struct itimerval *);
struct timezone;
extern int	gettimeofday(struct timeval *, struct timezone *);

extern int	fsync(int);
extern int	ftruncate(int, off_t);

extern char *crypt(char *, char *);

/* End of ultrix4.h */
