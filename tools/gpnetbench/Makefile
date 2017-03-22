# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
SHELL=/bin/bash

default: all

this_makefile := $(abspath $(lastword $(MAKEFILE_LIST)))
BLD_TOP := $(dir $(this_makefile))/../../

subdir=tools/gpnetbench
top_builddir=../../
include $(BLD_TOP)/src/Makefile.global

SERVER_OBJS=gpnetbenchServer.o
CLIENT_OBJS=gpnetbenchClient.o

all: gpnetbenchServer gpnetbenchClient
	@echo "finish building gpnetbenchServer and gpnetbenchClient"

gpnetbenchServer: $(SERVER_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o gpnetbenchServer $(SERVER_OBJS)

gpnetbenchClient: $(CLIENT_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o gpnetbenchClient $(CLIENT_OBJS)

install: all
	$(INSTALL_PROGRAM) gpnetbenchServer ${DESTDIR}$(bindir)/lib
	$(INSTALL_PROGRAM) gpnetbenchClient ${DESTDIR}$(bindir)/lib

%.o: %.c
	$(CC) $(CFLAGS) -o $@ -c $<

clean:
	rm -rf *.o gpnetbenchServer gpnetbenchClient

distclean: clean

.PHONY: default all clean distclean
