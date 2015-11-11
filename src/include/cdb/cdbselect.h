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
 * cdbselect.h
 *
 *	  Provides equivalents to FD_SET/FD_ZERO -- but handle more than
 *  1024 file descriptors (as far as I can tell all of our platforms
 *  support 65536).
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBSELECT_H
#define CDBSELECT_H

#if !defined(_WIN32) && \
	!(defined(pg_on_solaris) && defined(__sparc) && defined(_LP64))

/* 8-bit bytes 32 or 64-bit ints */

typedef struct
{
 	int32 __fds_bits[65536/(sizeof(int32) * 8)];
} mpp_fd_set;

#define MPP_FD_ZERO(setp) (memset((char *)(setp), 0, sizeof(mpp_fd_set)))

#define MPP_FD_WORD(fd) ((fd) >> 5)
#define MPP_FD_BIT(fd) (1 << ((fd) & 0x1f))

#define MPP_FD_SET(fd, set) do{ \
	if (fd > 65535) \
	elog(FATAL,"Internal error:  Using fd > 65535 in MPP_FD_SET"); \
	((set)->__fds_bits[MPP_FD_WORD(fd)] = ((set)->__fds_bits[MPP_FD_WORD(fd)]) | MPP_FD_BIT(fd)); \
    } while(0)
#define MPP_FD_CLR(fd, set) ((set)->__fds_bits[MPP_FD_WORD(fd)] &= ~MPP_FD_BIT(fd))

#define MPP_FD_ISSET(fd, set) (((set)->__fds_bits[MPP_FD_WORD(fd)] & MPP_FD_BIT(fd)) ? 1 : 0)

#else

#define mpp_fd_set fd_set
#define MPP_FD_ZERO(setp) FD_ZERO(setp)

#define MPP_FD_SET(fd, set) do{ \
	if (fd > FD_SETSIZE) \
	elog(FATAL,"Internal error:  Using fd > FD_SETSIZE in FD_SET (MPP_FD_SET)"); \
	FD_SET(fd, set); \
	} while(0)
#define MPP_FD_CLR(fd, set) FD_CLR(fd, set)

#define MPP_FD_ISSET(fd, set) FD_ISSET(fd, set)

#endif

#endif
