# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

OS = $(shell uname)

CXX = gcc 
CXXFLAGS = -Wall -O1 -g -std=gnu99 -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wformat-security -fno-strict-aliasing -fwrapv -fPIC

CC = gcc
CFLAGS = -Wall -O1 -g -std=gnu99 -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wformat-security -fno-strict-aliasing -fwrapv -fPIC

CPPFLAGS = -I$(abs_top_srcdir)/src/include
CPPFLAGS += -I$(abs_top_srcdir)/depends/libhdfs3/build/install$(prefix)/include
CPPFLAGS += -I$(abs_top_srcdir)/depends/libyarn/build/install$(prefix)/include

LDFLAGS = -L$(libdir)
LDFLAGS += -L$(abs_top_srcdir)/src/port
LDFLAGS += -L$(abs_top_builddir)/src/port
LDFLAGS += -L$(abs_top_srcdir)/depends/libhdfs3/build/install$(prefix)/lib
LDFLAGS += -L$(abs_top_srcdir)/depends/libyarn/build/install$(prefix)/lib

POSTGRES = $(abs_top_srcdir)/src/backend/postgres

PROG = function.c
OBJS = function.o
TARGET = function.so

RM = rm -rf 

all: $(TARGET)

$(OBJS): $(PROG)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $(OBJS) $(PROG)

$(TARGET): $(OBJS)
ifeq ($(OS),Darwin)
	$(CXX) $(CXXFLAGS) -bundle $(OBJS) -bundle_loader $(POSTGRES) $(LDFLAGS) -o $@
else
	$(CXX) $(CXXFLAGS) -shared $(OBJS) $(LDFLAGS) -Wl,-rpath,'$(abs_top_builddir)/lib' -o $@
endif

clean:
	$(RM) *.o
